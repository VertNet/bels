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

__author__ = "Marie-Elise Lecoq"
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__version__ = "api_test.py 2021-07-03T15:36-03:00"

from job import process_csv_in_bulk
from collections import namedtuple
import base64
import json

# Uploaded file in GCS where the input Location fields to be tested are located. 
### To test country code interpretation
upload_file_to_test = 'gs://localityservice/jobs/test_dual_countrycodes.csv'
event = {
  'data': {
    'upload_file_url': upload_file_to_test,
    'email': 'gtuco.btuco@gmail.com',
    'output_filename': 'test_jrw_from_local',
    'header': [ 'country', 'countryCode', 'v_countryCode']
  }
}
###

### To test real records with georeferences plus their predetermined results
upload_file_to_test = 'gs://localityservice/jobs/test_matchme_sans_coords_best_georef.csv'
event = {
  'data': {
    'upload_file_url': upload_file_to_test,
    'email': 'gtuco.btuco@gmail.com',
    'output_filename': 'test_jrw_from_local',
    'header': [ 'dwc_location_hash', 'v_highergeographyid', 'v_highergeography', 
        'v_continent', 'v_waterbody', 'v_islandgroup', 'v_island', 'v_country', 
        'v_countrycode', 'v_stateprovince', 'v_county', 'v_municipality', 'v_locality', 
        'v_verbatimlocality', 'v_minimumelevationinmeters', 'v_maximumelevationinmeters', 
        'v_verbatimelevation', 'v_minimumdepthinmeters', 'v_maximumdepthinmeters', 
        'v_verbatimdepth', 'v_minimumdistanceabovesurfaceinmeters', 
        'v_maximumdistanceabovesurfaceinmeters', 'v_locationaccordingto', 
        'v_locationremarks', 'v_decimallatitude', 'v_decimallongitude', 
        'v_geodeticdatum', 'v_coordinateuncertaintyinmeters', 'v_coordinateprecision', 
        'v_pointradiusspatialfit', 'v_verbatimcoordinates', 'v_verbatimlatitude', 
        'v_verbatimlongitude', 'v_verbatimcoordinatesystem', 'v_verbatimsrs', 
        'v_footprintwkt', 'v_footprintsrs', 'v_footprintspatialfit', 'v_georeferencedby', 
        'v_georeferenceddate', 'v_georeferenceprotocol', 'v_georeferencesources', 
        'v_georeferenceverificationstatus', 'v_georeferenceremarks', 
        'matchme_sans_coords', 'unc_numeric', 'center', 
        'best_interpreted_decimallongitude', 'best_interpreted_decimallatitude', 
        'best_interpreted_countrycode', 'best_georeferencedby', 'best_georeferenceddate', 
        'best_georeferenceprotocol', 'best_georeferencesources', 
        'best_georeferenceremarks', 'georef_score', 'georef_count', 'max_uncertainty', 
        'centroid_dist', 'min_centroid_dist', 'matchid']
  }
}

event = base64.b64encode(json.dumps(event).encode('utf-8'))
event = {'data':event}
Context = namedtuple('Context', 'event_id timestamp')
context = Context('1234', '1234')

# Process a file in Google Cloud Storage via BigQuery and back to GCS
process_csv_in_bulk(event, context)
#process_csv(event, context)
