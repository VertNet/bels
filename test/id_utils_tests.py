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
__version__ = "id_utils_tests.py 2021-01-08T17:15-03:00"

# This file contains unit tests for the functions in id_utils.
#
# Example:
#
# python id_utils_tests.py

from id_utils import simplify_diacritics
from id_utils import casefold_and_normalize
from id_utils import location_match_str
from id_utils import location_str
from id_utils import dwc_location_hash
from id_utils import remove_symbols
from id_utils import remove_whitespace
from id_utils import save_numbers
from id_utils import super_simplify
from dwca_utils import lower_dict_keys
from dwca_terms import locationmatchwithcoordstermlist
from dwca_terms import locationmatchverbatimcoordstermlist
from dwca_terms import locationmatchsanscoordstermlist
from decimal import *
import base64
import unittest

class IDUtilsTestFramework():
    def dispose(self):
        return True

class IDUtilsTestCase(unittest.TestCase):
    # testdatapath is the location of example files to test with
    testdatapath = '../data/tests/'
    # vocabpath is the location of vocabulary files to test with
    vocabpath = '../vocabularies/'

    # following are files used as input during the tests, don't remove these
    darwincloudfile = vocabpath + 'darwin_cloud.txt'

    def setUp(self):
        self.framework = IDUtilsTestFramework()
        self.maxDiff = 1000

    def tearDown(self):
        self.framework.dispose()
        self.framework = None

    def test_simplify_diacritics(self):
        print('Running test_simplify_diacritics')
        teststr='ábçdèfghïjkłmñoœpqrßtûvwxÿž'
        target='abcdefghijklmnoœpqrstuvwxyz'
        simpstr = simplify_diacritics(teststr)
        self.assertEqual(simpstr, target)
        
        teststr='Görlitz-Biesnitz, {Gartensparte ("Am Löhnschen Park"); Hecke am Spartenlokal "Zum Edelweiß": [ca.] 15 m hügelaufwärts vom Aushängeschild an der Seitenfront}'
        target= 'Gorlitz-Biesnitz, {Gartensparte ("Am Lohnschen Park"); Hecke am Spartenlokal "Zum Edelweis": [ca.] 15 m hugelaufwarts vom Aushangeschild an der Seitenfront}'
        simpstr = simplify_diacritics(teststr)
        self.assertEqual(simpstr, target)

    def test_remove_symbols(self):
        print('Running test_remove_symbols')
        teststr='’<>:‒–—―…!«»‐?‘’“”;⁄␠·&@*•^¤¢$€£¥₩₪†‡°¡¿¬#№%‰‱¶′§~¨_|¦⁂☞∴‽※}{\\]\\[\\"\\)\\('
        target=''
        simpstr=remove_symbols(teststr)
        self.assertEqual(simpstr, target)

        teststr='-+,/’<>:‒–—―…!«»‐?‘’“”;⁄␠·&@*•^¤¢$€£¥₩₪†‡°¡¿¬#№%‰‱¶′§~¨_|¦⁂☞∴‽※}{\\]\\[\\"\\)\\('
        target='-+,/'
        simpstr=remove_symbols(teststr)
        self.assertEqual(simpstr, target)
        
    def test_remove_whitespace(self):
        print('Running test_remove_whitespace')
        teststr='a  b  c  d  e  1,2 3.4 5/6 -7.0 +8,9 \t\v\r\n '
        target='abcde1,23.45/6-7.0+8,9'
        simpstr=remove_whitespace(teststr)
        self.assertEqual(simpstr, target)
        
    def test_save_numbers(self):
        print('Running test_save_numbers')
        teststr='a, b. c/ d- e+ 1,2 3.4 5/6 -7.0 +8,9'
        target='a  b  c  d  e  1,2 3.4 5/6 -7.0 +8,9'
        simpstr=save_numbers(teststr)
        self.assertEqual(simpstr, target)
        
    def test_super_simplify(self):
        print('Running test_super_simplify')
        teststr='1.5 mi. N., 6,6 km S.; 2-4 T 1/2 km up H/T; .5 mi. 9. 15 -+5m T-A'
        target='1.5min6,6kms2-4t1/2kmupht.5mi9.15+5mta'
        simpstr=super_simplify(teststr)
        self.assertEqual(simpstr, target)
        
    def test_casefold_and_normalize(self):
        print('Running test_casefold_and_normalize')
        teststr=u'\u0061\u0301\u2168\u0041\u030A\u2167 áBçDèFGHïJKłMñoœPQRßTûVWXÿž'
        target='áixåviii ábçdèfghïjkłmñoœpqrsstûvwxÿž'
        cn = casefold_and_normalize(teststr)
        self.assertEqual(cn, target)

#     def test_normalize_and_casefold(self):
#         print('testing normalize_and_casefold')
#         teststr=u'\u0061\u0301\u2168\u0041\u030A\u2167 áBçDèFGHïJKłMñoœPQRßTûVWXÿž'
#         target='áⅸåⅷ ábçdèfghïjkłmñoœpqrsstûvwxÿž'
#         nc = normalize_and_casefold(teststr)
#         self.assertEqual(nc, target)

    def test_dwc_location_hash(self):
        print('Running test_dwc_location_hash')
        darwincloudfile = self.darwincloudfile
        loc = { 
            'dwc:highergeographyid':'', 
            'dwc:highergeography':'', 
            'dwc:continent':'', 
            'dwc:waterbody':'', 
            'dwc:islandgroup':'', 
            'dwc:island':'', 
            'dwc:country':'', 
            'dwc:countrycode':'RU', 
            'dwc:stateprovince':'Moskovskaya oblast', 
            'dwc:county':'', 
            'dwc:municipality':'', 
            'dwc:locality':'', 
            'dwc:verbatimlocality':'', 
            'dwc:minimumelevationinmeters':'',
            'dwc:maximumelevationinmeters':'',
            'dwc:verbatimelevation':'', 
            'dwc:minimumdepthinmeters':'', 
            'dwc:maximumdepthinmeters':'', 
            'dwc:verbatimdepth':'', 
            'dwc:minimumdistanceabovesurfaceinmeters':'', 
            'dwc:maximumdistanceabovesurfaceinmeters':'', 
            'dwc:locationaccordingto':'', 
            'dwc:locationremarks':'', 
            'dwc:decimallatitude':'55,802706', 
            'dwc:decimallongitude':'37,423519', 
            'dwc:geodeticdatum':'WGS84', 
            'dwc:coordinateuncertaintyinmeters':'', 
            'dwc:coordinateprecision':'',
            'dwc:pointradiusspatialfit':'', 
            'dwc:verbatimcoordinates':'', 
            'dwc:verbatimlatitude':'', 
            'dwc:verbatimlongitude':'', 
            'dwc:verbatimcoordinatesystem':'decimal degrees', 
            'dwc:verbatimsrs':'',
            'dwc:footprintwkt':'', 
            'dwc:footprintsrs':'', 
            'dwc:footprintspatialfit':'', 
            'dwc:georeferencedby':'', 
            'dwc:georeferenceddate':'', 
            'dwc:georeferenceprotocol':'',
            'dwc:georeferencesources':'', 
            'dwc:georeferenceverificationstatus':'', 
            'dwc:georeferenceremarks':''
        }
        # TO_HEX(dwc_location_hash)
        # target='091e331428057bff5c6e44e9236a4288e84e989884ef866af1f9276e648848fc'
        # TO_BASE64(dwc_location_hash)
        target='CR4zFCgFe/9cbkTpI2pCiOhOmJiE74Zq8fknbmSISPw='
        locid=dwc_location_hash(loc, darwincloudfile)
        self.assertEqual(locid, target)
    
    def test_location_str(self):
        print('Running test_location_str')
        loc = { 
            'dwc:highergeographyid':'', 
            'dwc:highergeography':'', 
            'dwc:continent':'', 
            'dwc:waterbody':'', 
            'dwc:islandgroup':'', 
            'dwc:island':'', 
            'dwc:country':'', 
            'dwc:countrycode':'RU', 
            'dwc:stateprovince':'Moskovskaya oblast', 
            'dwc:county':'', 
            'dwc:municipality':'', 
            'dwc:locality':'', 
            'dwc:verbatimlocality':'', 
            'dwc:minimumelevationinmeters':'',
            'dwc:maximumelevationinmeters':'',
            'dwc:verbatimelevation':'', 
            'dwc:minimumdepthinmeters':'', 
            'dwc:maximumdepthinmeters':'', 
            'dwc:verbatimdepth':'', 
            'dwc:minimumdistanceabovesurfaceinmeters':'', 
            'dwc:maximumdistanceabovesurfaceinmeters':'', 
            'dwc:locationaccordingto':'', 
            'dwc:locationremarks':'', 
            'dwc:decimallatitude':'55,802706', 
            'dwc:decimallongitude':'37,423519', 
            'dwc:geodeticdatum':'WGS84', 
            'dwc:coordinateuncertaintyinmeters':'', 
            'dwc:coordinateprecision':'',
            'dwc:pointradiusspatialfit':'', 
            'dwc:verbatimcoordinates':'', 
            'dwc:verbatimlatitude':'', 
            'dwc:verbatimlongitude':'', 
            'dwc:verbatimcoordinatesystem':'decimal degrees', 
            'dwc:verbatimsrs':'',
            'dwc:footprintwkt':'', 
            'dwc:footprintsrs':'', 
            'dwc:footprintspatialfit':'', 
            'dwc:georeferencedby':'', 
            'dwc:georeferenceddate':'', 
            'dwc:georeferenceprotocol':'',
            'dwc:georeferencesources':'', 
            'dwc:georeferenceverificationstatus':'', 
            'dwc:georeferenceremarks':''
        }
        target='dwc:highergeographyiddwc:highergeographydwc:continentdwc:waterbodydwc:islandgroupdwc:islanddwc:countrydwc:countrycodeRUdwc:stateprovinceMoskovskaya oblastdwc:countydwc:municipalitydwc:localitydwc:verbatimlocalitydwc:minimumelevationinmetersdwc:maximumelevationinmetersdwc:verbatimelevationdwc:minimumdepthinmetersdwc:maximumdepthinmetersdwc:verbatimdepthdwc:minimumdistanceabovesurfaceinmetersdwc:maximumdistanceabovesurfaceinmetersdwc:locationaccordingtodwc:locationremarksdwc:decimallatitude55,802706dwc:decimallongitude37,423519dwc:geodeticdatumWGS84dwc:coordinateuncertaintyinmetersdwc:coordinateprecisiondwc:pointradiusspatialfitdwc:verbatimcoordinatesdwc:verbatimlatitudedwc:verbatimlongitudedwc:verbatimcoordinatesystemdecimal degreesdwc:verbatimsrsdwc:footprintwktdwc:footprintsrsdwc:footprintspatialfitdwc:georeferencedbydwc:georeferenceddatedwc:georeferenceprotocoldwc:georeferencesourcesdwc:georeferenceverificationstatusdwc:georeferenceremarks'
        locid=location_str(loc)
        self.assertEqual(locid, target)
    
    def test_location_match_str(self):
        print('Running test_location_match_str')
        loc = { 
            'continent':'1',
            'waterBody':'2', 
            'islandGroup':'3', 
            'island':'4', 
            'countryCode':'5', 
            'stateProvince':'6', 
            'county':'7', 
            'municipality':'8', 
            'locality':'9', 
            'verbatimlocality':'10', 
            'minimumelevationinmeters':'11',
            'maximumelevationinmeters':'12', 
            'verbatimelevation':'13', 
            'minimumdepthinmeters':'14', 
            'maximumdepthinmeters':'15', 
            'VERBATIMDEPTH':'16', 
            'verbatimcoordinates':'17', 
            'verbatimlatitude':'18', 
            'verbatimlongitude':'19',
            'decimallatitude': Decimal(20),
            'decimallongitude': Decimal(21)
        }
        lowerloc = lower_dict_keys(loc)
        locstr=location_match_str(locationmatchwithcoordstermlist, lowerloc)
        target='1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 '
        self.assertEqual(locstr, target)

        locstr=location_match_str(locationmatchverbatimcoordstermlist, lowerloc)
        target='1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 '
        self.assertEqual(locstr, target)

        locstr=location_match_str(locationmatchsanscoordstermlist, lowerloc)
        target='1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 '
        self.assertEqual(locstr, target)

        loc = { 
            'continent':'1',
            'waterBody':'2', 
            'islandGroup':'3', 
            'island':'4', 
            'countryCode':'5', 
            'stateProvince':'6', 
            'county':'7', 
            'municipality':'8', 
            'locality':'9', 
            'verbatimlocality':'10', 
            'minimumelevationinmeters':'11',
            'maximumelevationinmeters':'12', 
            'verbatimelevation':'13', 
            'minimumdepthinmeters':'14', 
            'maximumdepthinmeters':'15', 
            'VERBATIMDEPTH':'16', 
            'verbatimcoordinates':'17', 
            'verbatimlatitude':'18', 
            'verbatimlongitude':'19',
            'decimallatitude': Decimal(20.12345675),
            'decimallongitude': Decimal(-21.12345675)
        }
        lowerloc = lower_dict_keys(loc)
        locstr=location_match_str(locationmatchwithcoordstermlist, lowerloc)
        target='1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20.1234567 -21.1234567 '
        self.assertEqual(locstr, target)

#     def test_location_match_id_hex(self):
#         print('Running test_location_match_id')
#         teststr='Quién sabe?'
#         target='c1ceb2f1888d8763ed44861b644056fb96b9a501cb7764f65ffb1e3343ae036d'
#         id = location_match_id_hex(teststr)
#         self.assertEqual(id, target)
# 
#         ss = super_simplify(teststr)
#         target='412283b7ac7d301e19a3aef22adc0261145483a9310858f0d2dcad76558463d1'
#         id = location_match_id_hex(ss)
#         self.assertEqual(id, target)

if __name__ == '__main__':
    print('=== id_utils_test.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
