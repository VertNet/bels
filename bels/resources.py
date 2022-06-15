#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "John Wieczorek"
__contributors__ = ""
__copyright__ = "Copyright 2022 Rauthiflor LLC"
__filename__ = "resources.py"
__version__ = __filename__ + ' ' + "2022-06-08T15:52-03:00"

import base64
import logging
import os
import time
from flask import request
from flask_restful import Resource
from google.cloud import bigquery

from bels_query import bels_original_georef
from bels_query import get_best_sans_coords_georef_reduced
from bels_query import get_best_with_verbatim_coords_georef_reduced
from bels_query import get_best_with_coords_georef_reduced
from bels_query import has_decimal_coords
from bels_query import has_georef
from bels_query import has_verbatim_coords
from bels_query import row_as_dict
from dwca_terms import locationmatchsanscoordstermlist
from dwca_terms import locationmatchverbatimcoordstermlist
from dwca_terms import locationmatchwithcoordstermlist
from dwca_vocab_utils import Darwinizer
from dwca_utils import lower_dict_keys
from id_utils import location_match_str, super_simplify

class BestGeoref(Resource):
    def __init__(self, bels_client):
        self.__name__='BestGeoref'
        self.bels_client = bels_client
        self.bq_client = bels_client.bq_client
        self.darwinizer = Darwinizer('./bels/vocabularies/darwin_cloud.txt')
        logging.basicConfig(level=logging.DEBUG)

    def post(self):
        starttime = time.perf_counter()
        if request.is_json == False:
            response = {"Message": {"status": "error", "Result": f"Request is empty or is not valid JSON."}}
            logging.debug(f'BestGeoref request: {request}\nresponse: {response}')
            return response, 400

        requestjson = request.get_json()
        give_me = requestjson.get('give_me')
        if give_me is None:
            response = {"Message": {"status": "error", "Result": f"No 'give_me' directive in request: {requestjson}"}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 400
        
        if give_me.upper() not in ['BEST_GEOREF']:
            response = {"Message": {"status": "error", "Result": f"Directive {give_me.upper()} not supported"}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 400

        row = requestjson.get('row')
        if row is None or len(row)==0:
            response = {"Message": {"status": "error", "Result": f"No row data in request: {requestjson}"}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 400

        loc = self.darwinizer.darwinize_dict(row_as_dict(row))
        if loc is None:
            response = {"Message": {"status": "error", "Result": f"Darwinize failed for {requestjson}."}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 500

        lowerloc = lower_dict_keys(loc)
        if 'country' not in lowerloc and 'countrycode' not in lowerloc:
            response = {"Message": {"status": "error", "Result": f"No interpretable country field in : {requestjson}"}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 400

        # Short-cut if the row already has a georeference
        if has_georef(loc):
            row = bels_original_georef(lowerloc)
            row['bels_countrycode'] = self.bels_client.get_best_countrycode(lowerloc)
            response = {"Message": {"status": "success", "Result": row}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 200
        
        if self.bq_client is None:
            response = {"Message": {"status": "error", "Result": "No BigQuery client."}}
            logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
            return response, 500

        bestcountrycode = self.bels_client.get_best_countrycode(lowerloc)
        lowerloc['countrycode'] = bestcountrycode
        result = None
        matchstr = None
        if give_me == 'BEST_GEOREF':
            starttime = time.perf_counter()
            if has_decimal_coords(lowerloc) == True:
                matchme = location_match_str(locationmatchwithcoordstermlist, lowerloc)
                matchstr = super_simplify(matchme)
                result = get_best_with_coords_georef_reduced(self.bq_client, matchstr)
                if result is None:
                    print(f'No match with coords found {matchstr}')
            if result is None and has_verbatim_coords(lowerloc): 
                matchme = location_match_str(locationmatchverbatimcoordstermlist, lowerloc)
                matchstr = super_simplify(matchme)
                result = get_best_with_verbatim_coords_georef_reduced(self.bq_client, matchstr)
                if result is None:
                    print(f'No match with verbatim coords found {matchstr}')
            if result is None:
                matchme = location_match_str(locationmatchsanscoordstermlist, lowerloc)
                matchstr = super_simplify(matchme)
                result = get_best_sans_coords_georef_reduced(self.bq_client, matchstr)    
                if result is None:
                    print(f'No match sans coords found {matchstr}')

            querytime = time.perf_counter()-starttime
            if result:
                for field in ['dwc_location_hash', 'locationid']:
                    if field in result:
                        result[field] = base64.b64encode(result[field]).decode('utf-8')
                row.update(result)
                response = {"Message": {"status": "success", "elapsed_time": f'{querytime:1.3f}s', "Result": row}}
                logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
                return response, 200
            else:
                print(f'No georeference found')
                response = {"Message": {"status": "failure", "Result": row}}
                logging.debug(f'BestGeoref request: {requestjson}\nresponse: {response}')
                return response, 200
