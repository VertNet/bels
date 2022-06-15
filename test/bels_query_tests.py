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
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__filename__ = "bels_query_tests.py"
__version__ = __filename__ + ' ' + "2022-06-08T17:35-03:00"

# This file contains unit tests for the query functions in bels 
# (Biodiversity Enhanced Location Services).
#
# Example:
#
# python bels_query_tests.py
#
# Note: Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to point to the JSON
# file that contains the service account key (auth.json) if there are authentication 
# issues resulting in exceptions such as below (see Setting the Environment Variable at
# https://cloud.google.com/docs/authentication/getting-started):
#  File "/Users/johnwieczorek/.virtualenvs/bels/lib/python3.8/site-packages/google/oauth2/_client.py", 
#  line 60, in _handle_error_response
#  raise exceptions.RefreshError(error_details, response_data)
#  google.auth.exceptions.RefreshError: ('invalid_grant: Bad Request', 
#  {'error': 'invalid_grant', 'error_description': 'Bad Request'})

from decimal import *
import unittest
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../bels/auth.json"

from google.cloud import bigquery

from dwca_terms import locationmatchwithcoordstermlist
from dwca_terms import locationmatchsanscoordstermlist
from dwca_terms import locationmatchverbatimcoordstermlist
from dwca_terms import gbiflocationmatchwithcoordstermlist
from dwca_terms import gbiflocationmatchsanscoordstermlist
from dwca_terms import gbiflocationmatchverbatimcoordstermlist
from dwca_utils import safe_read_csv_row
from dwca_utils import lower_dict_keys
from dwca_vocab_utils import darwinize_dict
from dwca_vocab_utils import darwinize_list
from id_utils import dwc_location_hash
from id_utils import location_match_str
from id_utils import super_simplify
from bels_query import bels_original_georef
from bels_query import georeference_score
from bels_query import coordinates_score
from bels_query import get_best_sans_coords_georef
from bels_query import get_best_with_coords_georef
from bels_query import get_best_with_verbatim_coords_georef
from bels_query import get_best_sans_coords_georef_reduced
from bels_query import get_best_with_coords_georef_reduced
from bels_query import get_best_with_verbatim_coords_georef_reduced
from bels_query import get_location_by_id
from bels_query import get_location_by_hashid
from bels_query import has_georef
from bels_query import row_as_dict
from bels_query import bigquerify_header

class BELSQueryTestFramework():
    # testdatapath is the location of example files to test with
    testdatapath = '../data/tests/'
    # vocabpath is the location of vocabulary files to test with
    vocabpath = '../bels/vocabularies/'

    # following are files used as input during the tests, don't remove these
    darwincloudfile = vocabpath + 'darwin_cloud.txt'
    locationswithhashfile = testdatapath + 'test_locations_with_hash.csv'
    matchmesanscoordsbestgeoreffile = testdatapath + 'test_matchme_sans_coords_best_georef.csv'
    matchmewithcoordsbestgeoreffile = testdatapath + 'test_matchme_with_coords_best_georef.csv'
    matchmeverbatimcoordsbestgeoreffile = testdatapath + 'test_matchme_verbatimcoords_best_georef.csv'
    locsanscoordsbestgeoreffile = testdatapath + 'test_loc_with_sans_coords_best_georef.csv'
    locsanscoordsbestgeoreffilemulti = testdatapath + 'test_best_georefs_sans_coords_for_locations.csv'
    locswithcoordsbestgeoreffile = testdatapath + 'test_loc_with_with_coords_best_georef.csv'
    locswithverbatimcoordsbestgeoreffile = testdatapath + 'test_loc_with_verbatimcoords_best_georef.csv'

    def dispose(self):
        return True

class BELSQueryTestCase(unittest.TestCase):
    def setUp(self):
        self.framework = BELSQueryTestFramework()
        self.BQ = bigquery.Client()
        self.maxDiff = None

    def tearDown(self):
        self.framework.dispose()
        self.framework = None
        self.BQ.close()

    def test_georeference_score(self):
        print('Running test_georeference_score')

        inputrow = {'countrycode':'DK', 'locality':'Gudhjem'}
        result = georeference_score(inputrow)
        expected = 0
        self.assertEqual(result, expected)
        
        inputrow = {'georeferenceprotocol':'protocol', 'georeferencesources':'sources', \
            'georeferenceddate':'date', 'georeferencedby':'georefby', \
            'georeferenceremarks':'remarks'}
        result = georeference_score(inputrow)
        expected = 31
        self.assertEqual(result, expected)

        inputrow = {'georeferenceprotocol':'', 'georeferencesources':'', \
            'georeferenceddate':'', 'georeferencedby':'', 'georeferenceremarks':''}
        result = georeference_score(inputrow)
        expected = 0
        self.assertEqual(result, expected)

        inputrow = {'georeferenceprotocol':None, 'georeferencesources':None, \
            'georeferenceddate':None, 'georeferencedby':None, 'georeferenceremarks':None}
        result = georeference_score(inputrow)
        expected = 0
        self.assertEqual(result, expected)

    def test_coordinates_score(self):
        print('Running test_coordinates_score')

        inputrow = {'countrycode':'DK', 'locality':'Gudhjem'}
        result = coordinates_score(inputrow)
        expected = 0
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10'}
        result = coordinates_score(inputrow)
        expected = 224
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'200', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10'}
        result = coordinates_score(inputrow)
        expected = 96
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'-300', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10'}
        result = coordinates_score(inputrow)
        expected = 96
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'0'}
        result = coordinates_score(inputrow)
        expected = 192
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'', 'decimallongitude':'', \
            'geodeticdatum':'', 'coordinateuncertaintyinmeters':''}
        result = georeference_score(inputrow)
        expected = 0
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':None, 'decimallongitude':None, \
            'geodeticdatum':None, 'coordinateuncertaintyinmeters':None}
        result = coordinates_score(inputrow)
        expected = 0
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10', \
            'georeferenceprotocol':'protocol', 'georeferencesources':'sources', \
            'georeferenceddate':'date', 'georeferencedby':'georefby', \
            'georeferenceremarks':'remarks'}
        result = coordinates_score(inputrow)
        expected = 255
        self.assertEqual(result, expected)

    def test_has_georef(self):
        print('Running test_has_georef')

        inputrow = {'countrycode':'DK', 'locality':'Gudhjem'}
        result = has_georef(inputrow)
        expected = False
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10'}
        result = has_georef(inputrow)
        expected = True
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'200', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10'}
        result = has_georef(inputrow)
        expected = False
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'-300', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10'}
        result = has_georef(inputrow)
        expected = False
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'0'}
        result = has_georef(inputrow)
        expected = False
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'', 'decimallongitude':'', \
            'geodeticdatum':'', 'coordinateuncertaintyinmeters':''}
        result = has_georef(inputrow)
        expected = False
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':None, 'decimallongitude':None, \
            'geodeticdatum':None, 'coordinateuncertaintyinmeters':None}
        result = has_georef(inputrow)
        expected = False
        self.assertEqual(result, expected)

        inputrow = {'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10', \
            'georeferenceprotocol':'protocol', 'georeferencesources':'sources', \
            'georeferenceddate':'date', 'georeferencedby':'georefby', \
            'georeferenceremarks':'remarks'}
        result = has_georef(inputrow)
        expected = True
        self.assertEqual(result, expected)

    def test_bels_original_georef(self):
        print('Running test_bels_original_georef')

        inputrow = {'country':'Denmark', 'decimallatitude':'20', 'decimallongitude':'30', \
            'geodeticdatum':'epsg:4326', 'coordinateuncertaintyinmeters':'10', \
            'georeferenceprotocol':'protocol', 'georeferencesources':'sources', \
            'georeferenceddate':'date', 'georeferencedby':'georefby', \
            'georeferenceremarks':'remarks'}
        result = bels_original_georef(inputrow)
        expected = {
            'bels_countrycode':None, 'bels_match_string':None, 
            'bels_decimallatitude':'20', 'bels_decimallongitude':'30', 
            'bels_geodeticdatum':'epsg:4326', 'bels_coordinateuncertaintyinmeters':'10', 
            'bels_georeferencedby':'georefby', 'bels_georeferenceddate':'date',
            'bels_georeferenceprotocol':'protocol', 'bels_georeferencesources':'sources',
            'bels_georeferenceremarks':'remarks', 'bels_georeference_score':31, 
            'bels_georeference_source':'original data', 
            'bels_best_of_n_georeferences':1, 'bels_match_type':'original georeference'}
        self.assertEqual(result, expected)

    def test_bigquerify_header(self):
        print('Running test_bigquerify_header')
        input_fields = ['a', '1', '', '_', '$', u'ł', 'm"@#%', 'test', 'test', 'test', \
            'v_verbatimcoordinates\r', \
'0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', \
            'body_mass.1.ambiguous_key']
        target = ['a', '_1', '_3', '_', '__', 'ł', 'm____', 'test', '_test', '__test', \
            'v_verbatimcoordinates_', \
'_012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345', \
            'body_mass_1_ambiguous_key']
        result = bigquerify_header(input_fields)
        self.assertEqual(result, target)

    def test_bigquery_countries(self):
        print('Running test_bigquery_countries')
        input_fields = ['country', 'countrycode', 'countryCode', 'v_countrycode']
        target = ['country', 'countryCode', '_countryCode', 'v_countryCode']
        darwinized_fields = darwinize_list(input_fields, self.framework.darwincloudfile)
        result = bigquerify_header(darwinized_fields)
        self.assertEqual(result, target)

    def test_get_best_sans_coords_georef(self):
        print('Running test_get_best_sans_coords_georef')
        matchstr = 'auwac73kmsofbillabongroadhouse'
        result = get_best_sans_coords_georef(self.BQ, matchstr)
        #print(f'AU result: {result}')
        target = {
            'dwc_location_hash': b'\x03.\x8c\xc2$\xb17\xbb%\xef>\xa8Y\x01DQ\xb5F_\x9f\xf8\x89\xc5\xb3Y\xda\r\xc6i\xce\xbc\x1c',
            'v_highergeographyid': None,
            'v_highergeography': None,
            'v_continent': None,
            'v_waterbody': None,
            'v_islandgroup': None,
            'v_island': None,
            'v_country': 'Australia',
            'v_countrycode': None,
            'v_stateprovince': 'WA',
            'v_county': None,
            'v_municipality': None,
            'v_locality': 'c. 73 km S of Billabong Roadhouse',
            'v_verbatimlocality': None,
            'v_minimumelevationinmeters': None,
            'v_maximumelevationinmeters': None,
            'v_verbatimelevation': None,
            'v_verticaldatum':'',
            'v_minimumdepthinmeters': None,
            'v_maximumdepthinmeters': None,
            'v_verbatimdepth': None,
            'v_minimumdistanceabovesurfaceinmeters': '',
            'v_maximumdistanceabovesurfaceinmeters': '',
            'v_locationaccordingto': None,
            'v_locationremarks': None,
            'v_decimallatitude': '-27.48333333',
            'v_decimallongitude': '114.7',
            'v_geodeticdatum': 'GDA94',
            'v_coordinateuncertaintyinmeters': '10000',
            'v_coordinateprecision': None,
            'v_pointradiusspatialfit': None,
            'v_verbatimcoordinates': None,
            'v_verbatimlatitude': None,
            'v_verbatimlongitude': None,
            'v_verbatimcoordinatesystem': None,
            'v_verbatimsrs': None,
            'v_footprintwkt': None,
            'v_footprintsrs': None,
            'v_footprintspatialfit': None,
            'v_georeferencedby': None, 
            'v_georeferenceddate': None, 
            'v_georeferenceprotocol': None, 
            'v_georeferencesources': None, 
            'v_georeferenceremarks': None, 
            'interpreted_decimallongitude': 114.7, 
            'interpreted_decimallatitude': -27.48333333, 
            'interpreted_countrycode': 'AU', 
            'occcount': 1,
            'u_datumstr': 'GDA94',
            'tokens': 'australia wa c 73 km s of billabong roadhouse au',
            'matchme_with_coords': 'auwac73kmsofbillabongroadhouse-27.4833333114.7',
            'matchme': 'auwac73kmsofbillabongroadhouse',
            'matchme_sans_coords': 'auwac73kmsofbillabongroadhouse', 
            'epsg': 4283,
            'georef_score': 0, 
            'coordinates_score': 224,
            'source': 'iDigBio', 
            'unc_numeric': Decimal('10000'), 
            'bels_decimallatitude': Decimal('-27.4833333'),
            'bels_decimallongitude': Decimal('114.7'),
            'bels_coordinateuncertaintyinmeters': Decimal('10000'),
            'center': 'POINT(114.7 -27.4833333)', 
            'georef_count': 1
        }
        self.assertEqual(result, target)
        
        matchstr = 'idsulawesiutarapulaunainindonesiasulawesiutarapulaunain'
        result = get_best_sans_coords_georef(self.BQ, matchstr)
        #print(f'ID result: {result}')
        target = {
            'dwc_location_hash': b'h\x93\x0c\xe3\xb2\xc2\xa3\xee\x0e"P%v>2\x92\xf5\xceO\n\xe2)\xf0b\x99\x85\x05|!\xeb\x0c\xb7',
            'v_highergeographyid': 'http://vocab.getty.edu/tgn/1000116',
            'v_highergeography': 'Asia: Indonesia',
            'v_continent': 'Asia',
            'v_waterbody': None,
            'v_islandgroup': None,
            'v_island': None,
            'v_country': 'Indonesia',
            'v_countrycode': 'ID',
            'v_stateprovince': None,
            'v_county': None,
            'v_municipality': None,
            'v_locality': 'Sulawesi Utara,Pulau Nain',
            'v_verbatimlocality': 'Indonesia:Sulawesi Utara,Pulau Nain',
            'v_minimumelevationinmeters': None,
            'v_maximumelevationinmeters': None,
            'v_verbatimelevation': None,
            'v_verticaldatum': None,
            'v_minimumdepthinmeters': None,
            'v_maximumdepthinmeters': None,
            'v_verbatimdepth': None,
            'v_minimumdistanceabovesurfaceinmeters': None,
            'v_maximumdistanceabovesurfaceinmeters': None,
            'v_locationaccordingto': None,
            'v_locationremarks': None,
            'v_decimallatitude': '1.78333',
            'v_decimallongitude': '124.78333',
            'v_geodeticdatum': 'WGS84',
            'v_coordinateuncertaintyinmeters': '3615',
            'v_coordinateprecision': None,
            'v_pointradiusspatialfit': None,
            'v_verbatimcoordinates': None,
            'v_verbatimlatitude': None,
            'v_verbatimlongitude': None,
            'v_verbatimcoordinatesystem': 'decimal degrees',
            'v_verbatimsrs': None,
            'v_footprintwkt': None,
            'v_footprintsrs': None,
            'v_footprintspatialfit': None,
            'v_georeferencedby': 'JBH (MCZ)',
            'v_georeferenceddate': None,
            'v_georeferenceprotocol': 'MaNIS/HerpNET/ORNIS Georeferencing Guidelines',
            'v_georeferencesources': 'Gazetteer of Indonesia: US Defense Mapping Agency (1982)',
            'v_georeferenceremarks': 'Used Nain, ISL  Also known as Naeng-besar, Pulau.',
            'interpreted_decimallatitude': 1.78333,
            'interpreted_decimallongitude': 124.78333,
            'interpreted_countrycode': 'ID',
            'occcount': 41,
            'u_datumstr': 'WGS84',
            'tokens': 'asia asia indonesia indonesia id sulawesi utara pulau nain indonesiasulawesi utara pulau nain jbh mcz gazetteer of indonesia us defense mapping agency 1982 manis herpnet ornis georeferencing guidelines used nain isl also known as naeng besar pulau id',
            'matchme_with_coords': 'idsulawesiutarapulaunainindonesiasulawesiutarapulaunain1.78333124.78333',
            'matchme': 'idsulawesiutarapulaunainindonesiasulawesiutarapulaunain',
            'matchme_sans_coords': 'idsulawesiutarapulaunainindonesiasulawesiutarapulaunain',
            'epsg': 4326,
            'georef_score': 27,
            'coordinates_score': 251,
            'source': 'GBIF',
            'unc_numeric': Decimal('3615'),
            'bels_decimallatitude': Decimal('1.78333'),
            'bels_decimallongitude': Decimal('124.78333'),
            'bels_coordinateuncertaintyinmeters': Decimal('3615'),
            'center': 'POINT(124.78333 1.78333)',
            'georef_count': 2
        }
        self.assertEqual(result, target)

    def test_get_best_sans_coords_georef_reduced(self):
        print('Running test_get_best_sans_coords_georef_reduced')
        matchstr = 'auwac73kmsofbillabongroadhouse'
        result = get_best_sans_coords_georef_reduced(self.BQ, matchstr)
        #print(f'AU result reduced: {result}')
        target = {
            'bels_countrycode': 'AU', 
            'bels_match_string': 'auwac73kmsofbillabongroadhouse', 
            'bels_decimallatitude': -27.48333333, 
            'bels_decimallongitude': 114.7, 
            'bels_geodeticdatum':'epsg:4326',
            'bels_coordinateuncertaintyinmeters': 10000, 
            'bels_georeferencedby': None, 
            'bels_georeferenceddate': None, 
            'bels_georeferenceprotocol': None, 
            'bels_georeferencesources': None, 
            'bels_georeferenceremarks': None, 
            'bels_georeference_score': 0, 
            'bels_georeference_source': 'iDigBio', 
            'bels_best_of_n_georeferences': 1, 
            'bels_match_type':'match sans coords'
        }
        self.assertEqual(result, target)
        
        matchstr = 'idsulawesiutarapulaunainindonesiasulawesiutarapulaunain'
        result = get_best_sans_coords_georef_reduced(self.BQ, matchstr)
        #print(f'ID result reduced: {result}')
        target = {
            'bels_countrycode': 'ID', 
            'bels_match_string': 'idsulawesiutarapulaunainindonesiasulawesiutarapulaunain', 
            'bels_decimallatitude': 1.78333, 
            'bels_decimallongitude': 124.78333, 
            'bels_geodeticdatum':'epsg:4326',
            'bels_coordinateuncertaintyinmeters': 3615, 
            'bels_georeferencedby': 'JBH (MCZ)', 
            'bels_georeferenceddate': None, 
            'bels_georeferenceprotocol': 'MaNIS/HerpNET/ORNIS Georeferencing Guidelines', 
            'bels_georeferencesources': 'Gazetteer of Indonesia: US Defense Mapping Agency (1982)', 
            'bels_georeferenceremarks': 'Used Nain, ISL  Also known as Naeng-besar, Pulau.', 
            'bels_georeference_score': 27, 
            'bels_georeference_source': 'GBIF', 
            'bels_best_of_n_georeferences': 2, 
            'bels_match_type':'match sans coords'
        }
        self.assertEqual(result, target)

        matchstr = 'nowatthisshouldreturnaresult'
        result = get_best_sans_coords_georef_reduced(self.BQ, matchstr)
        target = None
        self.assertEqual(result, target)

    def test_get_best_with_coords_georef(self):
        print('Running test_get_best_with_coords_georef')
        matchstr = 'setukholmanorrstrom59.3281918.067591'
        result = get_best_with_coords_georef(self.BQ, matchstr)
        #print(f'SE result: {result}')
        target = {
            'dwc_location_hash': b"\xb8\x82\x92\x1c\xb5\xdc(\x03\xe2\rD\xd5'\xb9D\xe6\xe0\x11V\x8c%C\x17\xda]\x17o\x93o\xeai\xda",
            'v_highergeographyid': None,
            'v_highergeography': None,
            'v_continent': None,
            'v_waterbody': None,
            'v_islandgroup': None,
            'v_island': None,
            'v_country': None,
            'v_countrycode': None,
            'v_stateprovince': None,
            'v_county': None,
            'v_municipality': None,
            'v_locality': 'Tukholma, Norrström',
            'v_verbatimlocality': None,
            'v_minimumelevationinmeters': None,
            'v_maximumelevationinmeters': None,
            'v_verbatimelevation': None,
            'v_verticaldatum': None,
            'v_minimumdepthinmeters': None,
            'v_maximumdepthinmeters': None,
            'v_verbatimdepth': None,
            'v_minimumdistanceabovesurfaceinmeters': None,
            'v_maximumdistanceabovesurfaceinmeters': None,
            'v_locationaccordingto': None,
            'v_locationremarks': None,
            'v_decimallatitude': '59.32819',
            'v_decimallongitude': '18.067591',
            'v_geodeticdatum': 'EPSG:4326',
            'v_coordinateuncertaintyinmeters': '1',
            'v_coordinateprecision': None,
            'v_pointradiusspatialfit': None,
            'v_verbatimcoordinates': None,
            'v_verbatimlatitude': None,
            'v_verbatimlongitude': None,
            'v_verbatimcoordinatesystem': None,
            'v_verbatimsrs': None,
            'v_footprintwkt': 'POINT(18.067590594292 59.328190216457)',
            'v_footprintsrs': 'EPSG:4326',
            'v_footprintspatialfit': None,
            'v_georeferencedby': None,
            'v_georeferenceddate': None,
            'v_georeferenceprotocol': None,
            'v_georeferencesources': None,
            'v_georeferenceremarks': None,
            'interpreted_decimallatitude': 59.32819,
            'interpreted_decimallongitude': 18.067591,
            'interpreted_countrycode': 'SE',
            'occcount': 1,
            'u_datumstr': 'EPSG:4326',
            'tokens': 'tukholma norrstrom se',
            'matchme_with_coords': 'setukholmanorrstrom59.3281918.067591',
            'matchme': 'setukholmanorrstrom',
            'matchme_sans_coords': 'setukholmanorrstrom',
            'epsg': 4326,
            'georef_score': 0,
            'coordinates_score': 224,
            'source': 'GBIF',
            'unc_numeric': Decimal('1'),
            'bels_decimallatitude': Decimal('59.32819'),
            'bels_decimallongitude': Decimal('18.067591'),
            'bels_coordinateuncertaintyinmeters': Decimal('1'),
            'center': 'POINT(18.067591 59.32819)',
            'georef_count': 1
        }
        self.assertEqual(result, target)

    def test_get_best_with_coords_georef_reduced(self):
        print('Running test_get_best_with_coords_georef_reduced')
        matchstr = 'setukholmanorrstrom59.3281918.067591'
        result = get_best_with_coords_georef_reduced(self.BQ, matchstr)
        #print(f'SE result reduced: {result}')
        target = {
            'bels_countrycode': 'SE', 
            'bels_match_string': 'setukholmanorrstrom59.3281918.067591', 
            'bels_decimallatitude': 59.32819, 
            'bels_decimallongitude': 18.067591, 
            'bels_geodeticdatum':'epsg:4326',
            'bels_coordinateuncertaintyinmeters': 1, 
            'bels_georeferencedby': None, 
            'bels_georeferenceddate': None, 
            'bels_georeferenceprotocol': None, 
            'bels_georeferencesources': None, 
            'bels_georeferenceremarks': None, 
            'bels_georeference_score': 0, 
            'bels_georeference_source': 'GBIF', 
            'bels_best_of_n_georeferences': 1, 
            'bels_match_type':'match with coords'
        }
        self.assertEqual(result, target)

        matchstr = 'nowaythisshouldreturnaresult'
        result = get_best_with_coords_georef_reduced(self.BQ, matchstr)
        target = None
        self.assertEqual(result, target)

    def test_get_best_with_verbatim_coords_georef(self):
        print('Running test_get_best_with_verbatim_coords_georef')
        matchstr = 'usminnesotawadenat136nr33ws.1012-jul-71,'
#        matchid='YcHC5X1M3bUVjMAav1D2XYKLTLihxhGh1JDGs/C+m00='
        result = get_best_with_verbatim_coords_georef(self.BQ, matchstr)
        # print(f'US result: {result}')
        target = {
          'dwc_location_hash': b'\xd4\xeb\x9f=\x84k1\xaa=E\x98\x06nw"\xc3\xc0&\x11\x04#\xf8t\xa5r\xd2\x82\xb7\xf88%\xda',
          'v_highergeographyid': None,
          'v_highergeography': None,
          'v_continent': None,
          'v_waterbody': None,
          'v_islandgroup': None,
          'v_island': None,
          'v_country': 'United States',
          'v_countrycode': None,
          'v_stateprovince': 'Minnesota',
          'v_county': 'Wadena',
          'v_municipality': None,
          'v_locality': 'T136N, R33W, S.10',
          'v_verbatimlocality': None,
          'v_minimumelevationinmeters': None,
          'v_maximumelevationinmeters': None,
          'v_verbatimelevation': None,
          'v_verticaldatum': None,
          'v_minimumdepthinmeters': None,
          'v_maximumdepthinmeters': None,
          'v_verbatimdepth': None,
          'v_minimumdistanceabovesurfaceinmeters': None,
          'v_maximumdistanceabovesurfaceinmeters': None,
          'v_locationaccordingto': None,
          'v_locationremarks': None,
          'v_decimallatitude': '46.6081179',
          'v_decimallongitude': '-94.832611',
          'v_geodeticdatum': 'WGS84',
          'v_coordinateuncertaintyinmeters': '1137',
          'v_coordinateprecision': None,
          'v_pointradiusspatialfit': None,
          'v_verbatimcoordinates': '12-Jul-71,',
          'v_verbatimlatitude': None,
          'v_verbatimlongitude': None,
          'v_verbatimcoordinatesystem': None,
          'v_verbatimsrs': None,
          'v_footprintwkt': None,
          'v_footprintsrs': None,
          'v_footprintspatialfit': None,
          'v_georeferencedby': 'Lisa Strait (MSU)',
          'v_georeferenceddate': None,
          'v_georeferenceprotocol': 'MaNIS/HerpNet/ORNIS Georeferencing Guidelines, GBIF Best Practices',
          'v_georeferencesources': 'BioGeomancer',
          'v_georeferenceremarks': None,
          'interpreted_decimallatitude': 46.608118,
          'interpreted_decimallongitude': -94.832611,
          'interpreted_countrycode': 'US',
          'occcount': 1,
          'u_datumstr': 'WGS84',
          'tokens': 'united states minnesota wadena t136n r33w s.10 lisa strait msu biogeomancer manis herpnet ornis georeferencing guidelines gbif best practices us',
          'matchme_with_coords': 'usminnesotawadenat136nr33ws.1012-jul-71,46.6081179-94.832611',
          'matchme': 'usminnesotawadenat136nr33ws.1012-jul-71,',
          'matchme_sans_coords': 'usminnesotawadenat136nr33ws.10',
          'epsg': 4326,
          'georef_score': 26,
          'coordinates_score': 250,
          'source': 'GBIF',
          'unc_numeric': Decimal('1137'),
          'bels_decimallatitude': Decimal('46.608118'),
          'bels_decimallongitude': Decimal('-94.832611'),
          'bels_coordinateuncertaintyinmeters': Decimal('1137'),
          'center': 'POINT(-94.832611 46.608118)',
          'georef_count': 1
        }
        self.assertEqual(result, target)

    def test_matchme_sans_coords_best_georef_from_file(self):
        print('Running test_matchme_sans_coords_best_georef_from_file')
        inputfile = self.framework.locsanscoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print(f'test rowdict: {rowdict}')
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print(f'lowerloc: {lowerloc}')
            locmatchstr = location_match_str(locationmatchsanscoordstermlist, lowerloc)
            # print(f'locmatchstr: {locmatchstr}')
            matchstr=super_simplify(locmatchstr)
            # The result in the matchme_sans_coords field in the file was the one from 
            # processing in BELS where the countrycode was interpreted and included. 
            # Here we are not doing that step, so it shouldn't match the value of the 
            # matchme_sans_coords field.
            result = row['matchme_sans_coords']
            self.assertNotEqual(result, matchstr)
            result = 'wisconsinrichlandco5milesseofrichlandcenter'
            self.assertEqual(result, matchstr)

    def test_gbif_matchme_sans_coords_best_georef_from_file(self):
        print('Running test_gbif_matchme_sans_coords_best_georef_from_file')
        inputfile = self.framework.locsanscoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print(f'test rowdict: {rowdict}')
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print(f'lowerloc: {lowerloc}')
            locmatchstr = location_match_str(gbiflocationmatchsanscoordstermlist, lowerloc)
            # print(f'locmatchstr: {locmatchstr}')
            matchstr=super_simplify(locmatchstr)
            result = row['matchme_sans_coords']
            self.assertEqual(result, matchstr)

    def test_matchme_with_coords_best_georef_from_file(self):
        print('Running test_matchme_with_coords_best_georef_from_file')
        inputfile = self.framework.locswithcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print(f'test rowdict: {rowdict}')
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print(f'lowerloc: {lowerloc}')
            locmatchstr = location_match_str(locationmatchwithcoordstermlist, lowerloc)
            # print(f'locmatchstr: {locmatchstr}')
            matchstr=super_simplify(locmatchstr)
            # The result in the matchme_with_coords field in the file was the one from 
            # processing in BELS where the countrycode was interpreted and included. 
            # Here we are not doing that step, so it shouldn't match the value of the 
            # matchme_with_coords field.
            result = row['matchme_with_coords']
            self.assertNotEqual(result, matchstr)

            result = 'maranhaolagoverdefazendasaofranciscoestradaaltoalegrelagoverdekm9-3.9572-44.8219'
            self.assertEqual(result, matchstr)
#             # Only test the first row
            return
# Source file is test_loc_with_with_coords_best_georef.csv
# brmaranhaolagoverdefazendasaofranciscoestradaaltoalegrelagoverdekm9-3.9572-44.8219
# maranhaolagoverdefazendasaofranciscoestradaaltoalegrelagoverdekm9-3.9572-44.8219
#             ^

    def test_gbif_matchme_with_coords_best_georef_from_file(self):
        print('Running test_gbif_matchme_with_coords_best_georef_from_file')
        inputfile = self.framework.locswithcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print(f'test rowdict: {rowdict}')
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print(f'lowerloc: {lowerloc}')
            locmatchstr = location_match_str(gbiflocationmatchwithcoordstermlist, lowerloc)
            # print(f'locmatchstr: {locmatchstr}')
            matchstr=super_simplify(locmatchstr)
            result = row['matchme_with_coords']
            self.assertEqual(result, matchstr)
            # Only test the first row
            return

    def test_matchme_verbatimcoords_best_georef_from_file(self):
        print('Running test_matchme_verbatimcoords_best_georef_from_file')
        inputfile = self.framework.locswithverbatimcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print(f'test rowdict: {rowdict}')
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print(f'lowerloc: {lowerloc}')
            locmatchstr = location_match_str(locationmatchverbatimcoordstermlist, lowerloc)
            # print(f'locmatchstr: {locmatchstr}')
            matchstr=super_simplify(locmatchstr)
            # The result in the matchme field in the file was the one from 
            # processing in BELS where the countrycode was interpreted and included. 
            # Here we are not doing that step, so it shouldn't match the value of the 
            # matchme field.
            result = row['matchme']
            self.assertNotEqual(result, matchstr)
            result = 'chilelagopuyehuesumpfgelandeeinflusflusthermaspuyehuechilechilelagopuyehuesumpfgelandeeinflusflusthermaspuyehue-40,66667-72,46667'
            self.assertEqual(result, matchstr)
#             # Only test the first row
            return



    def test_gbif_matchme_verbatimcoords_best_georef_from_file(self):
        print('Running test_gbif_matchme_verbatimcoords_best_georef_from_file')
        inputfile = self.framework.locswithverbatimcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print(f'test rowdict: {rowdict}')
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print(f'lowerloc: {lowerloc}')
            locmatchstr = location_match_str(gbiflocationmatchverbatimcoordstermlist, lowerloc)
            # print(f'locmatchstr: {locmatchstr}')
            matchstr=super_simplify(locmatchstr)
            result = row['matchme']
            self.assertEqual(result, matchstr)
#             # Only test the first row
            return

if __name__ == '__main__':
    print('=== bels_query_tests.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
