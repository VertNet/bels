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
__filename__ = "id_utils_tests.py"
__version__ = "2022-05-31T20:33-03:00"

# This file contains unit tests for the functions in id_utils.
#
# Example:
#
# python id_utils_tests.py

from bels.id_utils import simplify_diacritics
from bels.id_utils import casefold_and_normalize
from bels.id_utils import location_match_str
from bels.id_utils import location_str
from bels.id_utils import dwc_location_hash
from bels.id_utils import remove_symbols
from bels.id_utils import remove_whitespace
from bels.id_utils import save_numbers
from bels.id_utils import super_simplify
from bels.dwca_utils import lower_dict_keys
from bels.dwca_terms import locationmatchwithcoordstermlist
from bels.dwca_terms import locationmatchverbatimcoordstermlist
from bels.dwca_terms import locationmatchsanscoordstermlist
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
    vocabpath = '../bels/vocabularies/'

    # following are files used as input during the tests, don't remove these
    darwincloudfile = vocabpath + 'darwin_cloud.txt'

    loc1 = { 
        'dummyfield':'',
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
        'dwc:locality':'unknown', 
        'dwc:verbatimlocality':'UNKNOWN ', 
        'dwc:minimumelevationinmeters':'',
        'dwc:maximumelevationinmeters':'',
        'dwc:verbatimelevation':'', 
        'dwc:verticaldatum':'', 
        'dwc:minimumdepthinmeters':'', 
        'dwc:maximumdepthinmeters':'', 
        'dwc:verbatimdepth':'', 
        'dummyfield2':'',
        'dwc:minimumdistanceabovesurfaceinmeters':'', 
        'dwc:maximumdistanceabovesurfaceinmeters':'', 
        'dwc:locationaccordingto':'', 
        'dwc:locationremarks':'', 
        'dwc:decimallatitude':'55.802706', 
        'dwc:decimallongitude':'37.423519', 
        'dwc:geodeticdatum':'WGS84', 
        'dwc:coordinateuncertaintyinmeters':'150', 
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
        'dwc:georeferenceremarks':'',
        'dummyfield3':''
    }
    loc2 = { 
        'dwc:countrycode':'RU', 
        'dwc:stateprovince':'Moskovskaya oblast', 
        'dwc:locality':'unknown', 
        'dwc:verbatimlocality':'UNKNOWN ', 
        'dwc:decimallatitude':'55,802706', 
        'dwc:decimallongitude':'37,423519', 
        'dwc:geodeticdatum':'WGS84', 
        'dwc:coordinateuncertaintyinmeters':'150', 
        'dwc:verbatimcoordinatesystem':'decimal degrees'
    }

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

    def test_dwc_location_hash(self):
        print('Running test_dwc_location_hash')
        darwincloudfile = self.darwincloudfile
        loc = self.loc1
        # TO_HEX(dwc_location_hash)
        # target='091e331428057bff5c6e44e9236a4288e84e989884ef866af1f9276e648848fc'
        # TO_BASE64(dwc_location_hash)
        target='vAb0q/J2UnKzuyfipHQGAZWb73pYW1soxmeRSB2iEdw='
        locid=dwc_location_hash(loc, darwincloudfile)
        self.assertEqual(locid, target)
    
    def test_location_str(self):
        print('Running test_location_str')
        loc = self.loc1
        target='dwc:highergeographyiddwc:highergeographydwc:continentdwc:waterbodydwc:islandgroupdwc:islanddwc:countrydwc:countrycodeRUdwc:stateprovinceMoskovskaya oblastdwc:countydwc:municipalitydwc:localityunknowndwc:verbatimlocalityUNKNOWN dwc:minimumelevationinmetersdwc:maximumelevationinmetersdwc:verbatimelevationdwc:verticaldatumdwc:minimumdepthinmetersdwc:maximumdepthinmetersdwc:verbatimdepthdwc:minimumdistanceabovesurfaceinmetersdwc:maximumdistanceabovesurfaceinmetersdwc:locationaccordingtodwc:locationremarksdwc:decimallatitude55.802706dwc:decimallongitude37.423519dwc:geodeticdatumWGS84dwc:coordinateuncertaintyinmeters150dwc:coordinateprecisiondwc:pointradiusspatialfitdwc:verbatimcoordinatesdwc:verbatimlatitudedwc:verbatimlongitudedwc:verbatimcoordinatesystemdecimal degreesdwc:verbatimsrsdwc:footprintwktdwc:footprintsrsdwc:footprintspatialfitdwc:georeferencedbydwc:georeferenceddatedwc:georeferenceprotocoldwc:georeferencesourcesdwc:georeferenceremarks'
        locid=location_str(loc)
        #print(f'locid:  {locid}\ntarget: {target}')
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
            'verticaldatum':'14', 
            'minimumdepthinmeters':'15', 
            'maximumdepthinmeters':'16', 
            'VERBATIMDEPTH':'17', 
            'verbatimcoordinates':'18', 
            'verbatimlatitude':'19', 
            'verbatimlongitude':'20',
            'decimallatitude': Decimal(21),
            'decimallongitude': Decimal(22)
        }
        lowerloc = lower_dict_keys(loc)
        locstr=location_match_str(locationmatchwithcoordstermlist, lowerloc)
        target='2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 '
        self.assertEqual(locstr, target)

        locstr=location_match_str(locationmatchverbatimcoordstermlist, lowerloc)
        target='2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 '
        self.assertEqual(locstr, target)

        locstr=location_match_str(locationmatchsanscoordstermlist, lowerloc)
        target='2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 '
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
            'verbatimlocality':'9', 
            'minimumelevationinmeters':'11',
            'maximumelevationinmeters':'12', 
            'verbatimelevation':'13', 
            'verticaldatum':'14', 
            'minimumdepthinmeters':'15', 
            'maximumdepthinmeters':'16', 
            'VERBATIMDEPTH':'17', 
            'verbatimcoordinates':'18', 
            'verbatimlatitude':'19', 
            'verbatimlongitude':'20',
            'decimallatitude': Decimal(21.12345675),
            'decimallongitude': Decimal(-22.12345675)
        }
        lowerloc = lower_dict_keys(loc)
        locstr=location_match_str(locationmatchwithcoordstermlist, lowerloc)
        target='2 3 4 5 6 7 8 9  11 12 13 14 15 16 17 18 19 20 21.1234567 -22.1234567 '
        self.assertEqual(locstr, target)

if __name__ == '__main__':
    print('=== id_utils_test.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
