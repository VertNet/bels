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
__copyright__ = "Copyright 2022 Rauthiflor LLC"
__filename__ = "bels_query_tests.py"
__version__ = __filename__ + ' ' + "2022-06-08T15:56-03:00"

# This file contains unit tests for the query functions in bels 
# (Biodiversity Enhanced Location Services).
#
# Example:
#
# python bels_query_tests.py

#from google.cloud import bigquery
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
from bels_query import row_as_dict
from bels_query import bigquerify_header
from dwca_utils import read_header
import unittest

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
#        self.BQ = bigquery.Client()
        self.maxDiff = None

    def tearDown(self):
        self.framework.dispose()
        self.framework = None
#        self.BQ.close()

    def test_header_2fieldsblank(self):
        print('Running test_header_2fieldsblank')
        csvreadheaderfile = '../data/tests/test_2fieldsblank.csv'
        header = read_header(csvreadheaderfile)
        #print(f'header: {header}')
        expected = ['continent','country','','','county','municipality','locality','verbatimlocality','minimumelevationinmeters','maximumelevationinmeters','verbatimelevation','v_locationaccordingto','v_locationremarks','v_verbatimcoordinates','v_verbatimlatitude','v_verbatimlongitude','v_verbatimcoordinatesystem']
        self.assertEqual(len(header), 17, 'incorrect number of fields in header')
        s = f'header:\n{header}\nnot as expected:\n{expected}'
        self.assertEqual(header, expected, s)

    def test_bigquerify_header(self):
        print('Running test_bigquerify_header')
        input_fields = ['continent','country','countrycode','stateprovince','county','municipality','locality','verbatimlocality','minimumelevationinmeters','maximumelevationinmeters','verbatimelevation','v_locationaccordingto','v_locationremarks','v_verbatimcoordinates','v_verbatimlatitude','v_verbatimlongitude','v_verbatimcoordinatesystem']
        target = ['continent','country','countrycode','stateprovince','county','municipality','locality','verbatimlocality','minimumelevationinmeters','maximumelevationinmeters','verbatimelevation','v_locationaccordingto','v_locationremarks','v_verbatimcoordinates','v_verbatimlatitude','v_verbatimlongitude','v_verbatimcoordinatesystem']
        result = bigquerify_header(input_fields)
        self.assertEqual(result, target)

        # This header currently fails to return results from the web app.
        input_fields = ['continent','country','','','county','municipality']
        target = ['continent','country','_3','_4','county','municipality']
        result = bigquerify_header(input_fields)
        self.assertEqual(result, target)

        # This header currently fails to return results from the web app.
        input_fields = ['continent','!country','1countrycode','stateprovince','county','municipality']
        target = ['continent','__country','_1countrycode','stateprovince','county','municipality']
        result = bigquerify_header(input_fields)
        #print(f'target: {target}\nresult: {result}')
        self.assertEqual(result, target)

        # This header currently fails to return results from the web app.
        input_fields = ['continent','1country','2countrycode','stateprovince','county','municipality']
        target = ['continent','_1country','_2countrycode','stateprovince','county','municipality']
        result = bigquerify_header(input_fields)
        #print(f'target: {target}\nresult: {result}')
        self.assertEqual(result, target)

        # This header currently fails to return results from the web app.
        input_fields = ['continent','!country','#countrycode','stateprovince','county','municipality']
        target = ['continent','__country','__countrycode','stateprovince','county','municipality']
        result = bigquerify_header(input_fields)
        #print(f'target: {target}\nresult: {result}')
        self.assertEqual(result, target)

        # This header currently fails to return results from the web app.
        input_fields = ['continent','!country','!countrycode','stateprovince','county','municipality']
        target = ['continent','__country','__countrycode','stateprovince','county','municipality']
        result = bigquerify_header(input_fields)
        #print(f'target: {target}\nresult: {result}')
        self.assertEqual(result, target)

        # This header currently fails to return results from the web app.
        input_fields = ['continent','country','!countrycode','4stateprovince','county','municipality']
        target = ['continent','country','__countrycode','_4stateprovince','county','municipality']
        result = bigquerify_header(input_fields)
        #print(f'target: {target}\nresult: {result}')
        self.assertEqual(result, target)

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

if __name__ == '__main__':
    print('=== bels_query_tests.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
