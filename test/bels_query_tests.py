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
__version__ = "bels_query_tests.py 2021-03-26T22:15-03:00"

# This file contains unit tests for the query functions in bels 
# (Biodiversity Enhanced Location Services).
#
# Example:
#
# python bels_query_tests.py

from google.cloud import bigquery
from bels.dwca_terms import locationmatchwithcoordstermlist
from bels.dwca_terms import locationmatchsanscoordstermlist
from bels.dwca_terms import locationmatchverbatimcoordstermlist
from bels.dwca_terms import gbiflocationmatchwithcoordstermlist
from bels.dwca_terms import gbiflocationmatchsanscoordstermlist
from bels.dwca_terms import gbiflocationmatchverbatimcoordstermlist
from bels.dwca_utils import safe_read_csv_row
from bels.dwca_utils import lower_dict_keys
from bels.dwca_vocab_utils import darwinize_dict
from bels.id_utils import dwc_location_hash
from bels.id_utils import location_match_str
from bels.id_utils import super_simplify
from bels.bels_query import get_best_sans_coords_georef
from bels.bels_query import get_best_with_coords_georef
from bels.bels_query import get_best_with_verbatim_coords_georef
from bels.bels_query import get_best_sans_coords_georef_reduced
from bels.bels_query import get_best_with_coords_georef_reduced
from bels.bels_query import get_best_with_verbatim_coords_georef_reduced
from bels.bels_query import get_location_by_id
from bels.bels_query import get_location_by_hashid
from bels.bels_query import row_as_dict
from decimal import *
import json
import unittest


class BELSQueryTestFramework():
    # testdatapath is the location of example files to test with
    testdatapath = '../data/tests/'
    # vocabpath is the location of vocabulary files to test with
    vocabpath = '../vocabularies/'

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

    def test_get_location_by_id(self):
        print('Running test_get_location_by_id')
        base64id='CR4zFCgFe/9cbkTpI2pCiOhOmJiE74Zq8fknbmSISPw='
        row = get_location_by_id(self.BQ,base64id)
#        print('row = %s' % row)
        target = b"\t\x1e3\x14(\x05{\xff\\nD\xe9#jB\x88\xe8N\x98\x98\x84\xef\x86j\xf1\xf9'nd\x88H\xfc"
        result = row['dwc_location_hash']
        self.assertEqual(result, target)

    def test_get_location_by_hashid(self):
        print('Running test_get_location_by_hashid')
        hashid = b"\t\x1e3\x14(\x05{\xff\\nD\xe9#jB\x88\xe8N\x98\x98\x84\xef\x86j\xf1\xf9'nd\x88H\xfc"
        row = get_location_by_hashid(self.BQ,hashid)
#        print('row = %s' % row)
#        target='CR4zFCgFe/9cbkTpI2pCiOhOmJiE74Zq8fknbmSISPw='
        target=hashid
        result = row['dwc_location_hash']
        self.assertEqual(result, target)

    def test_get_best_sans_coords_georef(self):
        print('Running test_get_best_sans_coords_georef')
        matchstr = 'auwac73kmsofbillabongroadhouse'
#        matchid='3CKYu8SB2PDattd8KYrAn6w4b6rNmlJzCKB4PVxHJwY='
        result = get_best_sans_coords_georef(self.BQ, matchstr)
        # print('AU: %s' % result)
        target = {
            'matchme_sans_coords': 'auwac73kmsofbillabongroadhouse', 
            'unc_numeric': Decimal('10000'), 
            'center': 'POINT(114.7 -27.48333333)', 
            'interpreted_decimallongitude': 114.7, 
            'interpreted_decimallatitude': -27.48333333, 
            'interpreted_countrycode': 'AU', 
            'v_georeferencedby': None, 
            'v_georeferenceddate': None, 
            'v_georeferenceprotocol': None, 
            'v_georeferencesources': None, 
            'v_georeferenceremarks': None, 
            'georef_score': 0, 
            'source': 'iDigBio', 
            'georef_count': 1, 
            'max_uncertainty': Decimal('10000'), 
            'centroid_dist': 0.0, 
            'min_centroid_dist': 0.0, 
            'matchid': b'\xdc"\x98\xbb\xc4\x81\xd8\xf0\xda\xb6\xd7|)\x8a\xc0\x9f\xac8o\xaa\xcd\x9aRs\x08\xa0x=\\G\'\x06'
        }
        self.assertEqual(result, target)
        
        matchstr = 'asiaidsulawesiutarapulaunainindonesiasulawesiutarapulaunain'
#        matchid = 'stPXsf74ZDnGF6wBRiMoyq8ku5b0xmnzGP1IK/nK0wU=''
        result = get_best_sans_coords_georef(self.BQ, matchstr)
        #print('ID: %s' % result)
        target = {
            'matchme_sans_coords': 'asiaidsulawesiutarapulaunainindonesiasulawesiutarapulaunain',
            'unc_numeric': Decimal('3615'),
            'center': 'POINT(124.78333 1.78333)',
            'interpreted_decimallongitude': 124.78333,
            'interpreted_decimallatitude': 1.78333,
            'interpreted_countrycode': 'ID',
            'v_georeferencedby': 'JBH (MCZ)',
            'v_georeferenceddate': None,
            'v_georeferenceprotocol': 'MaNIS/HerpNET/ORNIS Georeferencing Guidelines',
            'v_georeferencesources': 'Gazetteer of Indonesia: US Defense Mapping Agency (1982)',
            'v_georeferenceremarks': 'Used Nain, ISL  Also known as Naeng-besar, Pulau.',
            'georef_score': 27,
            'source': 'GBIF',
            'georef_count': 1,
            'max_uncertainty': Decimal('3615'),
            'centroid_dist': 0.0,
            'min_centroid_dist': 0.0,
            'matchid': b'\xb2\xd3\xd7\xb1\xfe\xf8d9\xc6\x17\xac\x01F#(\xca\xaf$\xbb\x96\xf4\xc6i\xf3\x18\xfdH+\xf9\xca\xd3\x05'
        }
        self.assertEqual(result, target)

    def test_get_best_sans_coords_georef_reduced(self):
        print('Running test_get_best_sans_coords_georef_reduced')
        matchstr = 'auwac73kmsofbillabongroadhouse'
#        matchid='3CKYu8SB2PDattd8KYrAn6w4b6rNmlJzCKB4PVxHJwY='
        result = get_best_sans_coords_georef_reduced(self.BQ, matchstr)
        target = {
            'sans_coords_match_string': 'auwac73kmsofbillabongroadhouse', 
            'sans_coords_countrycode': 'AU', 
            'sans_coords_decimallatitude': -27.48333333, 
            'sans_coords_decimallongitude': 114.7, 
            'sans_coords_coordinateuncertaintyinmeters': Decimal('10000'), 
            'sans_coords_georeferencedby': None, 
            'sans_coords_georeferenceddate': None, 
            'sans_coords_georeferenceprotocol': None, 
            'sans_coords_georeferencesources': None, 
            'sans_coords_georeferenceremarks': None, 
            'sans_coords_georef_score': 0, 
            'sans_coords_centroid_distanceinmeters': 0.0, 
            'sans_coords_georef_count': 1, 
        }
        self.assertEqual(result, target)
        
        matchstr = 'asiaidsulawesiutarapulaunainindonesiasulawesiutarapulaunain'
#        matchid = 'stPXsf74ZDnGF6wBRiMoyq8ku5b0xmnzGP1IK/nK0wU=''
        result = get_best_sans_coords_georef_reduced(self.BQ, matchstr)
        target = {
            'sans_coords_match_string': 'asiaidsulawesiutarapulaunainindonesiasulawesiutarapulaunain',
            'sans_coords_countrycode': 'ID',
            'sans_coords_decimallatitude': 1.78333,
            'sans_coords_decimallongitude': 124.78333,
            'sans_coords_coordinateuncertaintyinmeters': Decimal('3615'),
            'sans_coords_georeferencedby': 'JBH (MCZ)',
            'sans_coords_georeferenceddate': None,
            'sans_coords_georeferenceprotocol': 'MaNIS/HerpNET/ORNIS Georeferencing Guidelines',
            'sans_coords_georeferencesources': 'Gazetteer of Indonesia: US Defense Mapping Agency (1982)',
            'sans_coords_georeferenceremarks': 'Used Nain, ISL  Also known as Naeng-besar, Pulau.',
            'sans_coords_georef_score': 27,
            'sans_coords_centroid_distanceinmeters': 0.0,
            'sans_coords_georef_count': 1,
        }
        self.assertEqual(result, target)

        matchstr = 'nowatthisshouldreturnaresult'
        result = get_best_sans_coords_georef_reduced(self.BQ, matchstr)
        target = {
            'sans_coords_match_string': None,
            'sans_coords_countrycode': None,
            'sans_coords_decimallatitude': None,
            'sans_coords_decimallongitude': None,
            'sans_coords_coordinateuncertaintyinmeters': None,
            'sans_coords_georeferencedby': None,
            'sans_coords_georeferenceddate': None,
            'sans_coords_georeferenceprotocol': None,
            'sans_coords_georeferencesources': None,
            'sans_coords_georeferenceremarks': None,
            'sans_coords_georef_score': None,
            'sans_coords_centroid_distanceinmeters': None,
            'sans_coords_georef_count': None,
        }
        self.assertEqual(result, target)

    def test_get_best_with_coords_georef(self):
        print('Running test_get_best_with_coords_georef')
        matchstr = 'aqbechervaiseisland00-66.49559.49'
#        matchid='WXAe63f0h1LKMujroFLYoRVY03vWmCdvQynV5Y/9wUg='
        result = get_best_with_coords_georef(self.BQ, matchstr)
        #print('AQ: %s' % result)
        target = {
            'matchme_with_coords': 'aqbechervaiseisland00-66.49559.49', 
            'unc_numeric': Decimal('5000'), 
            'center': 'POINT(59.49 -66.495)', 
            'interpreted_decimallongitude': 59.49, 
            'interpreted_decimallatitude': -66.495, 
            'interpreted_countrycode': 'AQ', 
            'v_georeferencedby': None, 
            'v_georeferenceddate': None, 
            'v_georeferenceprotocol': None, 
            'v_georeferencesources': None, 
            'v_georeferenceremarks': None, 
            'georef_score': 0, 
            'source': 'GBIF',
            'georef_count': 1, 
            'max_uncertainty': Decimal('5000'), 
            'centroid_dist': 0.0, 
            'min_centroid_dist': 0.0, 
            'matchid': b'Yp\x1e\xebw\xf4\x87R\xca2\xe8\xeb\xa0R\xd8\xa1\x15X\xd3{\xd6\x98\'oC)\xd5\xe5\x8f\xfd\xc1H'
        }
        self.assertEqual(result, target)

    def test_get_best_with_coords_georef_reduced(self):
        print('Running test_get_best_with_coords_georef_reduced')
        matchstr = 'aqbechervaiseisland00-66.49559.49'
#        matchid='WXAe63f0h1LKMujroFLYoRVY03vWmCdvQynV5Y/9wUg='
        result = get_best_with_coords_georef_reduced(self.BQ, matchstr)
        target = {
            'with_coords_match_string': 'aqbechervaiseisland00-66.49559.49', 
            'with_coords_countrycode': 'AQ', 
            'with_coords_decimallatitude': -66.495, 
            'with_coords_decimallongitude': 59.49, 
            'with_coords_coordinateuncertaintyinmeters': Decimal('5000'), 
            'with_coords_georeferencedby': None, 
            'with_coords_georeferenceddate': None, 
            'with_coords_georeferenceprotocol': None, 
            'with_coords_georeferencesources': None, 
            'with_coords_georeferenceremarks': None, 
            'with_coords_georef_score': 0, 
            'with_coords_centroid_distanceinmeters': 0.0, 
            'with_coords_georef_count': 1, 
        }
        self.assertEqual(result, target)

        matchstr = 'nowatthisshouldreturnaresult'
        result = get_best_with_coords_georef_reduced(self.BQ, matchstr)
        target = {
            'sans_coords_match_string': None,
            'sans_coords_countrycode': None,
            'sans_coords_decimallatitude': None,
            'sans_coords_decimallongitude': None,
            'sans_coords_coordinateuncertaintyinmeters': None,
            'sans_coords_georeferencedby': None,
            'sans_coords_georeferenceddate': None,
            'sans_coords_georeferenceprotocol': None,
            'sans_coords_georeferencesources': None,
            'sans_coords_georeferenceremarks': None,
            'sans_coords_georef_score': None,
            'sans_coords_centroid_distanceinmeters': None,
            'sans_coords_georef_count': None,
        }
        self.assertEqual(result, target)

    def test_get_best_with_verbatim_coords_georef(self):
        print('Running test_get_best_with_verbatim_coords_georef')
        matchstr = 'usminnesotawadenat136nr33ws.1012-jul-71,'
#        matchid='YcHC5X1M3bUVjMAav1D2XYKLTLihxhGh1JDGs/C+m00='
        result = get_best_with_verbatim_coords_georef(self.BQ, matchstr)
        #print('US: %s' % result)
        target = {
            'matchme': 'usminnesotawadenat136nr33ws.1012-jul-71,', 
            'unc_numeric': Decimal('1137'), 
            'center': 'POINT(-94.832611 46.608118)', 
            'interpreted_decimallongitude': -94.832611, 
            'interpreted_decimallatitude': 46.608118, 
            'interpreted_countrycode': 'US', 
            'v_georeferencedby': 'Lisa Strait (MSU)', 
            'v_georeferenceddate': None, 
            'v_georeferenceprotocol': 'MaNIS/HerpNet/ORNIS Georeferencing Guidelines, GBIF Best Practices', 
            'v_georeferencesources': 'BioGeomancer', 
            'v_georeferenceremarks': None, 
            'georef_score': 26, 
            'source': 'GBIF',
            'georef_count': 1, 
            'max_uncertainty': Decimal('1137'), 
            'centroid_dist': 1.0042073245224715e-09, 
            'min_centroid_dist': 1.0042073245224715e-09, 
            'matchid': b'a\xc1\xc2\xe5}L\xdd\xb5\x15\x8c\xc0\x1a\xbfP\xf6]\x82\x8bL\xb8\xa1\xc6\x11\xa1\xd4\x90\xc6\xb3\xf0\xbe\x9bM'
        }
        self.assertEqual(result, target)

    def test_dwc_location_hash_from_safe_read_csv_row(self):
        print('Running test_dwc_location_hash_from_safe_read_csv_row')
        inputfile = self.framework.locationswithhashfile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            target = row['dwc_location_hash']
            result = dwc_location_hash(row, darwincloudfile)
            self.assertEqual(result, target)

    def test_matchme_sans_coords_best_georef_from_file(self):
        print('Running test_matchme_sans_coords_best_georef_from_file')
        inputfile = self.framework.locsanscoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(locationmatchsanscoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
            matchstr=super_simplify(locmatchstr)
            result = row['matchme_sans_coords']
            self.assertEqual(result, matchstr)

    def test_gbif_matchme_sans_coords_best_georef_from_file(self):
        print('Running test_gbif_matchme_sans_coords_best_georef_from_file')
        inputfile = self.framework.locsanscoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(gbiflocationmatchsanscoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
            matchstr=super_simplify(locmatchstr)
            result = row['matchme_sans_coords']
            self.assertEqual(result, matchstr)

    def test_gbif_matchme_sans_coords_best_georef_from_file2(self):
# oJZtuoTYbMjQ1IrdBWgFroLdzCxhWP2ShR3gdNwE7ko=
# bAcqFnoqie3GGRqoFvmKWXcuhKbnmOKrTq7W8XOzoRg=
# b8AuI53mC0Ke9+oDwHwzFsBgHVghA9TaX1pEAg8mX6s=
# FkKiS3RwTOLByklF2yq0dYpSvLqEZj+dU5HNJvSPL/8=
# 
# saconarinoricaurtereservanaturallaplanada
# northamericaqueencharlotteislandcabritishcolumbianorthamericacanadabritishcolumbia
# novestlandsunnfjordgrepstadvedbygdevegenhjajohangrepstad230
# dkvalloslotspark
        print('Running test_gbif_matchme_sans_coords_best_georef_from_file2')
        inputfile = self.framework.locsanscoordsbestgeoreffilemulti
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(gbiflocationmatchsanscoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
            matchstr=super_simplify(locmatchstr)
            result = get_best_sans_coords_georef(self.BQ, matchstr)
#            print('matchstr: %s best_sans_coords_georef: %s' % (matchstr,result))
            result = row['matchme_sans_coords']
            self.assertEqual(result, matchstr)

    def test_matchme_with_coords_best_georef_from_file(self):
        print('Running test_matchme_with_coords_best_georef_from_file')
        inputfile = self.framework.locswithcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(locationmatchwithcoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
            matchstr=super_simplify(locmatchstr)
            result = row['matchme_with_coords']
            self.assertEqual(result, matchstr)
#             # Only test the first row
            return

    def test_gbif_matchme_with_coords_best_georef_from_file(self):
        print('Running test_gbif_matchme_with_coords_best_georef_from_file')
        inputfile = self.framework.locswithcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(gbiflocationmatchwithcoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
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
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(locationmatchverbatimcoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
            matchstr=super_simplify(locmatchstr)
            result = row['matchme']
            self.assertEqual(result, matchstr)
#             # Only test the first row
            return

    def test_gbif_matchme_verbatimcoords_best_georef_from_file(self):
        print('Running test_gbif_matchme_verbatimcoords_best_georef_from_file')
        inputfile = self.framework.locswithverbatimcoordsbestgeoreffile
        darwincloudfile = self.framework.darwincloudfile
        for row in safe_read_csv_row(inputfile):
            rowdict = row_as_dict(row)
            # print('test rowdict: %s' % rowdict)
            loc = darwinize_dict(row_as_dict(row), darwincloudfile)
            lowerloc = lower_dict_keys(loc)
            # print('lowerloc: %s' % lowerloc)
            locmatchstr = location_match_str(gbiflocationmatchverbatimcoordstermlist, lowerloc)
            # print('locmatchstr: %s' % locmatchstr)
            matchstr=super_simplify(locmatchstr)
            result = row['matchme']
            self.assertEqual(result, matchstr)
#             # Only test the first row
            return

if __name__ == '__main__':
    print('=== bels_query_tests.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
