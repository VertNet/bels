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
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__filename__ = "resources.py"
__version__ = __filename__ + ' ' + "2021-10-03T14:44-03:00"

import os
import base64
import time

from bels.bels_query import bels_original_georef
from bels.bels_query import get_best_sans_coords_georef_reduced
from bels.bels_query import get_best_with_verbatim_coords_georef_reduced
from bels.bels_query import get_best_with_coords_georef_reduced
from bels.bels_query import has_decimal_coords
from bels.bels_query import has_georef
from bels.bels_query import has_verbatim_coords
from bels.bels_query import row_as_dict
from bels.dwca_terms import locationmatchsanscoordstermlist
from bels.dwca_terms import locationmatchverbatimcoordstermlist
from bels.dwca_terms import locationmatchwithcoordstermlist
from bels.dwca_vocab_utils import Darwinizer
from bels.dwca_utils import lower_dict_keys
from bels.id_utils import location_match_str, super_simplify

from flask import request
from flask_restful import Resource

from google.cloud import bigquery

class BestGeoref(Resource):
    def __init__(self, bels_client):
        self.__name__='BestGeoref'
        self.bels_client = bels_client
        self.bq_client = bels_client.bq_client
        self.darwinizer = Darwinizer('./vocabularies/darwin_cloud.txt')

    def post(self):
        starttime = time.perf_counter()
        if request.is_json == False:
            return {"Message": {"status": "error", "Result": f"Request is empty or is not valid JSON."}}, 400

        requestjson = request.get_json()
        give_me = requestjson.get('give_me')
        if give_me is None:
            return {"Message": {"status": "error", "Result": f"No 'give_me' directive in request: {requestjson}"}}, 400
        
        if give_me.upper() not in ['BEST_GEOREF']:
            return {"Message": {"status": "error", "Result": f"Directive {give_me.upper()} not supported"}}, 400

        row = requestjson.get('row')
        if row is None or len(row)==0:
            return {"Message": {"status": "error", "Result": f"No row data in request: {requestjson}"}}, 400

        loc = self.darwinizer.darwinize_dict(row_as_dict(row))
        if loc is None:
            return {"Message": {"status": "error", "Result": f"Darwinize failed for {requestjson}."}}, 500

        lowerloc = lower_dict_keys(loc)
        if 'country' not in lowerloc and 'countrycode' not in lowerloc:
            return {"Message": {"status": "error", "Result": f"No interpretable country field in : {requestjson}"}}, 400

        # Short-cut if the row already has a georeference
        if has_georef(loc):
            row = bels_original_georef(lowerloc)
            row['bels_countrycode'] = self.bels_client.get_best_countrycode(lowerloc)
            return {"Message": {"status": "success", "Result": row}}, 200
        
        if self.bq_client is None:
            return {"Message": {"status": "error", "Result": "No BigQuery client."}}, 500

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
            print(f'Execution time: {querytime:1.3f}s')

            if result:
                for field in ['dwc_location_hash', 'locationid']:
                    if field in result:
                        result[field] = base64.b64encode(result[field]).decode('utf-8')
                row.update(result)
                return {"Message": {"status": "success", "Result": row}}, 200
            else:
                print(f'No georeference found')
                return {"Message": {"status": "failure", "Result": {row}}}, 200
