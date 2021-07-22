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
__filename__ = "api_test.py"
__version__ = __filename__ + ' ' + "2021-07-22T15:51-03:00"

from bels.job import process_csv_in_bulk
from collections import namedtuple
import base64
import json
import logging

logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)


# This test relies on files already accessible in Google Cloud Storage. A few of these 
# have been prepared in advance for testing. Part of the preparation is to provide
# the header for the file, as the processing of that is otherwise done by api.py, which 
# isn't invoked here

# Uploaded files in GCS where the input Location fields to be tested are located. 

# upload_file_to_test = 'gs://localityservice/idigbio_2021-02-13a13.csv.gz'
# event = {
#   'data': {
#     'upload_file_url': upload_file_to_test,
#     'email': 'gtuco.btuco@gmail.com',
#     'output_filename': 'test_idigbio_from_local',
#     'header': [ 'coreid', 'continent', 'coordinateprecision', 
#     'coordinateuncertaintyinmeters', 'country', 'countrycode', 'county',
#     'decimallatitude', 'decimallongitude', 'footprintsrs', 'footprintspatialfit', 
#     'footprintwkt', 'geodeticdatum', 'georeferenceprotocol', 'georeferenceremarks', 
#     'georeferencesources', 'georeferenceverificationstatus', 'georeferencedby',
#     'georeferenceddate', 'highergeography', 'highergeographyid', 'island',
#     'islandgroup', 'locality', 'locationaccordingto', 'locationid', 'locationremarks',
#     'maximumdepthinmeters', 'maximumelevationinmeters', 'minimumdepthinmeters',
#     'minimumelevationinmeters', 'municipality', 'pointradiusspatialfit', 'stateprovince',
#     'verbatimcoordinatesystem', 'verbatimcoordinates', 'verbatimdepth', 
#     'verbatimelevation', 'verbatimlatitude', 'verbatimlocality', 'verbatimlongitude', 
#     'verbatimsrs', 'waterbody', 'idigbio_countrycode', 'idigbio_decimallatitude_wgs84',
#     'idigbio_decimallongitude_wgs84']
#   }
# }
## 6825038 records. Size: 430MB Format: CSV Compression: GZIP Prep: 0.42s Import: 209s 
## Georef: 596s Export: 257s Elapsed: 852s

# upload_file_to_test = 'gs://localityservice/Geographyexport.csv'
# event = {
#   'data': {
#     'upload_file_url': upload_file_to_test,
#     'email': 'gtuco.btuco@gmail.com',
#     'output_filename': 'test_output_jrw_geographyexport.csv',
#     'header': [ 'key', 'checked', 'incorrectable', 'verbatimMunicipality', 
#     'verbatimCounty', 'verbatimStateProvince', 'verbatimCountry', 'verbatimContinent',
#     'verbatimWaterBody', 'verbatimIslandGroup', 'verbatimIsland', 'municipality', 
#     'county', 'stateprovince', 'country', 'continent', 'waterbody', 'islandgroup',
#     'island', 'countrycode', 'error' ,'notHigherGeography', 'higherGeography']
#   }
# }
## 197994 records. Size: 36.4MB Format: CSV Compression: None Prep: 0.42s Import: 20s 
## Georef: 62s Export: 27s Elapsed: 89s

# upload_file_to_test = 'gs://localityservice/UFHerpsForBELSMatch.csv'
# event = {
#   'data': {
#     'upload_file_url': upload_file_to_test,
#     'email': 'gtuco.btuco@gmail.com',
#     'output_filename': 'test_output_jrw_ufherpsforbels.csv',
#     'header': [ 'localityid', 'v_locality', 'v_verbatimlatitude', 
#         'v_verbatimlongitude','v_highergeography', 'v_continent', 'v_country', 
#         'v_stateprovince', 'v_county', 'interpreted_countrycode']
#   }
# }
## 17576 records. Size: 2.3MB Format: CSV Compression: None Prep: 0.42s Import: 9s 
## Georef: 23s Export: 12s Elapsed: 34s

# upload_file_to_test = 'gs://localityservice/jobs/test_matchme_sans_coords_best_georef.csv'
# event = {
#   'data': {
#     'upload_file_url': upload_file_to_test,
#     'email': 'gtuco.btuco@gmail.com',
#     'output_filename': 'test_jrw_from_local',
#     'header': [ 'dwc_location_hash', 'v_highergeographyid', 'v_highergeography', 
#         'v_continent', 'v_waterbody', 'v_islandgroup', 'v_island', 'v_country', 
#         'v_countrycode', 'v_stateprovince', 'v_county', 'v_municipality', 'v_locality', 
#         'v_verbatimlocality', 'v_minimumelevationinmeters', 'v_maximumelevationinmeters', 
#         'v_verbatimelevation', 'v_minimumdepthinmeters', 'v_maximumdepthinmeters', 
#         'v_verbatimdepth', 'v_minimumdistanceabovesurfaceinmeters', 
#         'v_maximumdistanceabovesurfaceinmeters', 'v_locationaccordingto', 
#         'v_locationremarks', 'v_decimallatitude', 'v_decimallongitude', 
#         'v_geodeticdatum', 'v_coordinateuncertaintyinmeters', 'v_coordinateprecision', 
#         'v_pointradiusspatialfit', 'v_verbatimcoordinates', 'v_verbatimlatitude', 
#         'v_verbatimlongitude', 'v_verbatimcoordinatesystem', 'v_verbatimsrs', 
#         'v_footprintwkt', 'v_footprintsrs', 'v_footprintspatialfit', 'v_georeferencedby', 
#         'v_georeferenceddate', 'v_georeferenceprotocol', 'v_georeferencesources', 
#         'v_georeferenceverificationstatus', 'v_georeferenceremarks', 
#         'matchme_sans_coords', 'unc_numeric', 'center', 
#         'best_interpreted_decimallongitude', 'best_interpreted_decimallatitude', 
#         'best_interpreted_countrycode', 'best_georeferencedby', 'best_georeferenceddate', 
#         'best_georeferenceprotocol', 'best_georeferencesources', 
#         'best_georeferenceremarks', 'georef_score', 'georef_count', 'max_uncertainty', 
#         'centroid_dist', 'min_centroid_dist', 'matchid']
#   }
# }
## 10 records. Size: 5.5KB Format: CSV Compression: None Prep: 0.42s Import: 12s 
## Georef: 27s Export: 14s Elapsed: 40s

upload_file_to_test = 'gs://localityservice/jobs/test_demo.csv'
event = {
  'data': {
    'upload_file_url': upload_file_to_test,
    'email': 'gtuco.btuco@gmail.com',
    'output_filename': 'test_demo',
    'header': [ 'continent', 'country', 'countrycode', 'stateprovince', 'county', 
        'municipality', 'locality', 'verbatimlocality', 'minimumelevationinmeters', 
        'maximumelevationinmeters', 'verbatimelevation', 'v_locationaccordingto', 
        'v_locationremarks', 'v_verbatimcoordinates', 'v_verbatimlatitude', 
        'v_verbatimlongitude', 'v_verbatimcoordinatesystem']
  }
}
## 10 records. Size: 5.5KB Format: CSV Compression: None Prep: 0.001s Import: 11s 
## Georef: 22s Export: 13s Elapsed: 33s

event = base64.b64encode(json.dumps(event).encode('utf-8'))
event = {'data':event}
Context = namedtuple('Context', 'event_id timestamp')
context = Context('1234', '1234')

# Process a file in Google Cloud Storage via BigQuery and back to GCS
process_csv_in_bulk(event, context)
#process_csv(event, context)
