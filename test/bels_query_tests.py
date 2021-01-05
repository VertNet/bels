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
__version__ = "bels_query_tests.py 2021-01-05T01:04-03:00"

# This file contains unit tests for the query functions in bels 
# (Biodiversity Enhanced Location Services).
#
# Example:
#
# python bels_query_tests.py

from google.cloud import bigquery
from dwca_terms import locationmatchwithcoordstermlist
from dwca_terms import locationmatchsanscoordstermlist
from bels_query import get_best_sans_coords_georef
from bels_query import get_best_with_coords_georef
from bels_query import get_best_with_verbatim_coords_georef
from decimal import *
import json
import base64
import unittest

class BELSQueryTestFramework():
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

    def test_get_best_sans_coords_georef(self):
        matchstr = 'auwac73kmsofbillabongroadhouse'
#        matchid='3CKYu8SB2PDattd8KYrAn6w4b6rNmlJzCKB4PVxHJwY='
        result = get_best_sans_coords_georef(self.BQ, matchstr)
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
            'georef_count': 1,
            'max_uncertainty': Decimal('3615'),
            'centroid_dist': 0.0,
            'min_centroid_dist': 0.0,
            'matchid': b'\xb2\xd3\xd7\xb1\xfe\xf8d9\xc6\x17\xac\x01F#(\xca\xaf$\xbb\x96\xf4\xc6i\xf3\x18\xfdH+\xf9\xca\xd3\x05'
        }
        self.assertEqual(result, target)

    def test_get_best_with_coords_georef(self):
        matchstr = 'aqbechervaiseisland00-66.49559.49'
#        matchid='WXAe63f0h1LKMujroFLYoRVY03vWmCdvQynV5Y/9wUg='
        result = get_best_with_coords_georef(self.BQ, matchstr)
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
            'georef_count': 1, 
            'max_uncertainty': Decimal('5000'), 
            'centroid_dist': 0.0, 
            'min_centroid_dist': 0.0, 
            'matchid': b'Yp\x1e\xebw\xf4\x87R\xca2\xe8\xeb\xa0R\xd8\xa1\x15X\xd3{\xd6\x98\'oC)\xd5\xe5\x8f\xfd\xc1H'
        }
        self.assertEqual(result, target)

    def test_get_best_with_verbatim_coords_georef(self):
        matchstr = 'usminnesotawadenat136nr33ws.1012-jul-71,'
#        matchid='YcHC5X1M3bUVjMAav1D2XYKLTLihxhGh1JDGs/C+m00='
        result = get_best_with_verbatim_coords_georef(self.BQ, matchstr)
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
            'georef_count': 1, 
            'max_uncertainty': Decimal('1137'), 
            'centroid_dist': 1.0042073245224715e-09, 
            'min_centroid_dist': 1.0042073245224715e-09, 
            'matchid': b'a\xc1\xc2\xe5}L\xdd\xb5\x15\x8c\xc0\x1a\xbfP\xf6]\x82\x8bL\xb8\xa1\xc6\x11\xa1\xd4\x90\xc6\xb3\xf0\xbe\x9bM'
        }
        self.assertEqual(result, target)

if __name__ == '__main__':
    print('=== id_utils_test.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
