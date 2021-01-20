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
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__version__ = "job_tests.py 2021-01-19"

# This file contains unit tests for the job functions in bels 
# (Biodiversity Enhanced Location Services).
#
# Example:
#
# python job_tests.py


import unittest
from google.cloud import bigquery

from bels.job import confirm_hash_big_query


class BELSQueryTestFramework():
    # testdatapath is the location of example files to test with
    testdatapath = '../data/tests/'
    # vocabpath is the location of vocabulary files to test with
    vocabpath = '../vocabularies/'
    # following are files used as input during the tests, don't remove these
    locationswithhashfile = testdatapath + 'test_locations_with_hash.csv'

    def dispose(self):
        return True

class JobTestCase(unittest.TestCase):
    def setUp(self):
        self.framework = BELSQueryTestFramework()
        self.bq = bigquery.Client()

    def tearDown(self):
        self.framework.dispose()
        self.framework = None
        self.bq.close()

    def test_confirm_hash_big_query(self):
        print('Running confirm_hash_big_query')
        inputfile = self.framework.locationswithhashfile
        result = confirm_hash_big_query(self.bq, inputfile)
        target = [{'dwc_location_hash': 'lQLt+OOmqcpm6lxfjiPcx8VqaCalzgQEQwDges8Tnwg=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'United States', 'dwc:countrycode': '', 'dwc:stateprovince': 'Kansas', 'dwc:county': 'Marion', 'dwc:municipality': '', 'dwc:locality': 'Quail Creek Rd. (38.2936, -97.0586)', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '38.2936817', 'dwc:decimallongitude': '-97.0586498', 'dwc:geodeticdatum': 'WGS84', 'dwc:coordinateuncertaintyinmeters': '', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'lQLt+OOmqcpm6lxfjiPcx8VqaCalzgQEQwDges8Tnwg=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'United States', 'v_countrycode': None, 'v_stateprovince': 'Kansas', 'v_county': 'Marion', 'v_municipality': None, 'v_locality': 'Quail Creek Rd. (38.2936, -97.0586)', 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '38.2936817', 'v_decimallongitude': '-97.0586498', 'v_geodeticdatum': 'WGS84', 'v_coordinateuncertaintyinmeters': None, 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': 38.293682, 'interpreted_decimallongitude': -97.05865, 'interpreted_countrycode': 'US', 'occcount': 51, 'u_datumstr': 'WGS84', 'tokens': 'kansas marion quail creek rd 38.2936, -97.0586 us', 'matchme_with_coords': 'uskansasmarionquailcreekrd38.2936,-97.058638.2936817-97.0586498', 'matchme': 'uskansasmarionquailcreekrd38.2936,-97.0586', 'matchme_sans_coords': 'uskansasmarionquailcreekrd38.2936,-97.0586', 'epsg': 4326, 'georef_score': 0, 'coordinates_score': 224, 'source': 'GBIF'}, {'dwc_location_hash': '4NluL7SLQPmbJa3Ot2awpV6IRQAlTS12ZQvtP8a0MpQ=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'Australia', 'dwc:countrycode': '', 'dwc:stateprovince': 'Northern Territory', 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': 'Munyana Island, English Company Isles', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '-11.962', 'dwc:decimallongitude': '136.251', 'dwc:geodeticdatum': '', 'dwc:coordinateuncertaintyinmeters': '', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': '4NluL7SLQPmbJa3Ot2awpV6IRQAlTS12ZQvtP8a0MpQ=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'Australia', 'v_countrycode': None, 'v_stateprovince': 'Northern Territory', 'v_county': None, 'v_municipality': None, 'v_locality': 'Munyana Island, English Company Isles', 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '-11.962', 'v_decimallongitude': '136.251', 'v_geodeticdatum': None, 'v_coordinateuncertaintyinmeters': None, 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': -11.962, 'interpreted_decimallongitude': 136.251, 'interpreted_countrycode': 'AU', 'occcount': 1, 'u_datumstr': None, 'tokens': 'northern territory munyana island english company isles au', 'matchme_with_coords': 'aunorthernterritorymunyanaislandenglishcompanyisles-11.962136.251', 'matchme': 'aunorthernterritorymunyanaislandenglishcompanyisles', 'matchme_sans_coords': 'aunorthernterritorymunyanaislandenglishcompanyisles', 'epsg': None, 'georef_score': 0, 'coordinates_score': 160, 'source': 'GBIF'}, {'dwc_location_hash': 'dLq/BsjikRHos9ifyeUZHThkulaMPgkdsY/GsrRvDYU=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': 'Europe', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'Grèce', 'dwc:countrycode': '', 'dwc:stateprovince': '', 'dwc:county': '', 'dwc:municipality': 'Rhodes', 'dwc:locality': '', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '', 'dwc:decimallongitude': '', 'dwc:geodeticdatum': '', 'dwc:coordinateuncertaintyinmeters': '', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'dLq/BsjikRHos9ifyeUZHThkulaMPgkdsY/GsrRvDYU=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': 'Europe', 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'Grèce', 'v_countrycode': None, 'v_stateprovince': None, 'v_county': None, 'v_municipality': 'Rhodes', 'v_locality': None, 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': None, 'v_decimallongitude': None, 'v_geodeticdatum': None, 'v_coordinateuncertaintyinmeters': None, 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': None, 'interpreted_decimallongitude': None, 'interpreted_countrycode': 'GR', 'occcount': 410, 'u_datumstr': None, 'tokens': 'europe rhodes gr', 'matchme_with_coords': 'europegrrhodes', 'matchme': 'europegrrhodes', 'matchme_sans_coords': 'europegrrhodes', 'epsg': None, 'georef_score': 0, 'coordinates_score': 32, 'source': 'GBIF'}, {'dwc_location_hash': 'aKyFn79b5nfZ0wEOn+ly4/GY4j6EdZ51JcboQbMchTM=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': '', 'dwc:countrycode': '', 'dwc:stateprovince': '', 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': '', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '0', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '1032', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '50.747472', 'dwc:decimallongitude': '3.1843515', 'dwc:geodeticdatum': 'WGS84', 'dwc:coordinateuncertaintyinmeters': '9', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': 'GPS', 'dwc:georeferenceverificationstatus': 'unverified', 'dwc:georeferenceremarks': '', 'locationid': 'aKyFn79b5nfZ0wEOn+ly4/GY4j6EdZ51JcboQbMchTM=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': None, 'v_countrycode': None, 'v_stateprovince': None, 'v_county': None, 'v_municipality': None, 'v_locality': None, 'v_verbatimlocality': None, 'v_minimumelevationinmeters': '0', 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': '1032', 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '50.747472', 'v_decimallongitude': '3.1843515', 'v_geodeticdatum': 'WGS84', 'v_coordinateuncertaintyinmeters': '9', 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': 'GPS', 'v_georeferenceverificationstatus': 'unverified', 'v_georeferenceremarks': None, 'interpreted_decimallatitude': 50.747472, 'interpreted_decimallongitude': 3.184352, 'interpreted_countrycode': 'BE', 'occcount': 1, 'u_datumstr': 'WGS84', 'tokens': 'gps be', 'matchme_with_coords': 'be050.7474723.1843515', 'matchme': 'be0', 'matchme_sans_coords': 'be0', 'epsg': 4326, 'georef_score': 8, 'coordinates_score': 200, 'source': 'GBIF'}, {'dwc_location_hash': 'iQx3Mhir0Tt6H0ysSBlTV1p3EQsdRaZJWQwYLO2zcJc=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': '', 'dwc:countrycode': 'US', 'dwc:stateprovince': 'California', 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': '', 'dwc:verbatimlocality': 'Claremont, CA, USA', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '34.1105988863', 'dwc:decimallongitude': '-117.7152101733', 'dwc:geodeticdatum': '', 'dwc:coordinateuncertaintyinmeters': '2', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'iQx3Mhir0Tt6H0ysSBlTV1p3EQsdRaZJWQwYLO2zcJc=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': None, 'v_countrycode': 'US', 'v_stateprovince': 'California', 'v_county': None, 'v_municipality': None, 'v_locality': None, 'v_verbatimlocality': 'Claremont, CA, USA', 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '34.1105988863', 'v_decimallongitude': '-117.7152101733', 'v_geodeticdatum': None, 'v_coordinateuncertaintyinmeters': '2', 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': 34.110599, 'interpreted_decimallongitude': -117.71521, 'interpreted_countrycode': 'US', 'occcount': 1, 'u_datumstr': None, 'tokens': 'us california claremont ca usa us', 'matchme_with_coords': 'uscaliforniaclaremontcausa34.1105989-117.7152102', 'matchme': 'uscaliforniaclaremontcausa', 'matchme_sans_coords': 'uscaliforniaclaremontcausa', 'epsg': None, 'georef_score': 0, 'coordinates_score': 128, 'source': 'GBIF'}, {'dwc_location_hash': 'F9imbdlehgJ1KmLO8mPoukIEzk9s43E0/HcJsJxuw9A=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': 'Europe', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'France', 'dwc:countrycode': '', 'dwc:stateprovince': "Provence-Alpes-Côte d'Azur", 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': '1 km NW of col de Turini, Tête de Scoubayoun.', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '43.9845', 'dwc:decimallongitude': '7.379333', 'dwc:geodeticdatum': 'WGS84', 'dwc:coordinateuncertaintyinmeters': '', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'F9imbdlehgJ1KmLO8mPoukIEzk9s43E0/HcJsJxuw9A=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': 'Europe', 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'France', 'v_countrycode': None, 'v_stateprovince': "Provence-Alpes-Côte d'Azur", 'v_county': None, 'v_municipality': None, 'v_locality': '1 km NW of col de Turini, Tête de Scoubayoun.', 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '43.9845', 'v_decimallongitude': '7.379333', 'v_geodeticdatum': 'WGS84', 'v_coordinateuncertaintyinmeters': None, 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': 43.9845, 'interpreted_decimallongitude': 7.379333, 'interpreted_countrycode': 'FR', 'occcount': 2, 'u_datumstr': 'WGS84', 'tokens': 'europe provence alpes cote dazur 1 km nw of col de turini tete de scoubayoun fr', 'matchme_with_coords': 'europefrprovencealpescotedazur1kmnwofcoldeturinitetedescoubayoun43.98457.379333', 'matchme': 'europefrprovencealpescotedazur1kmnwofcoldeturinitetedescoubayoun', 'matchme_sans_coords': 'europefrprovencealpescotedazur1kmnwofcoldeturinitetedescoubayoun', 'epsg': 4326, 'georef_score': 0, 'coordinates_score': 224, 'source': 'GBIF'}, {'dwc_location_hash': 'ZO4lhVJS1J//eLhxRkTERfq0YfHLJAhIMLZfeMiQMrU=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'Unknown', 'dwc:countrycode': '', 'dwc:stateprovince': '', 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': "Galliæ et Germaniæ, Pelouses schisteuses, aux environs d'Angers.", 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '', 'dwc:decimallongitude': '', 'dwc:geodeticdatum': 'WGS84', 'dwc:coordinateuncertaintyinmeters': '', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'ZO4lhVJS1J//eLhxRkTERfq0YfHLJAhIMLZfeMiQMrU=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'Unknown', 'v_countrycode': None, 'v_stateprovince': None, 'v_county': None, 'v_municipality': None, 'v_locality': "Galliæ et Germaniæ, Pelouses schisteuses, aux environs d'Angers.", 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': None, 'v_decimallongitude': None, 'v_geodeticdatum': 'WGS84', 'v_coordinateuncertaintyinmeters': None, 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': None, 'interpreted_decimallongitude': None, 'interpreted_countrycode': 'ZZ', 'occcount': 1, 'u_datumstr': 'WGS84', 'tokens': 'galliae et germaniae pelouses schisteuses aux environs dangers zz', 'matchme_with_coords': 'zzgalliaeetgermaniaepelousesschisteusesauxenvironsdangers', 'matchme': 'zzgalliaeetgermaniaepelousesschisteusesauxenvironsdangers', 'matchme_sans_coords': 'zzgalliaeetgermaniaepelousesschisteusesauxenvironsdangers', 'epsg': 4326, 'georef_score': 0, 'coordinates_score': 96, 'source': 'GBIF'}, {'dwc_location_hash': 'ocRAS45J3iMgA6VdtxjLIBAU/Y5IBl3g7LPlbMfV1SI=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'Australia', 'dwc:countrycode': '', 'dwc:stateprovince': 'New South Wales', 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': 'locality withheld', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': 'locationRemarks withheld', 'dwc:decimallatitude': '-33.54253339', 'dwc:decimallongitude': '151.28232708', 'dwc:geodeticdatum': 'EPSG:4326', 'dwc:coordinateuncertaintyinmeters': '5.0', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'ocRAS45J3iMgA6VdtxjLIBAU/Y5IBl3g7LPlbMfV1SI=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'Australia', 'v_countrycode': None, 'v_stateprovince': 'New South Wales', 'v_county': None, 'v_municipality': None, 'v_locality': 'locality withheld', 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': 'locationRemarks withheld', 'v_decimallatitude': '-33.54253339', 'v_decimallongitude': '151.28232708', 'v_geodeticdatum': 'EPSG:4326', 'v_coordinateuncertaintyinmeters': '5.0', 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': -33.542533, 'interpreted_decimallongitude': 151.282327, 'interpreted_countrycode': 'AU', 'occcount': 1, 'u_datumstr': 'EPSG:4326', 'tokens': 'new south wales locality withheld locationremarks withheld au', 'matchme_with_coords': 'aunewsouthwaleslocalitywithheld-33.5425334151.2823271', 'matchme': 'aunewsouthwaleslocalitywithheld', 'matchme_sans_coords': 'aunewsouthwaleslocalitywithheld', 'epsg': 4326, 'georef_score': 0, 'coordinates_score': 192, 'source': 'GBIF'}, {'dwc_location_hash': '17ttwdeYUrpVPPyja0eE5Hw2mjT6tp8SxPwIbla55CE=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': '', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': '', 'dwc:countrycode': '', 'dwc:stateprovince': '', 'dwc:county': '', 'dwc:municipality': '', 'dwc:locality': '', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '37.943527', 'dwc:decimallongitude': '-7.012607', 'dwc:geodeticdatum': '', 'dwc:coordinateuncertaintyinmeters': '1', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '30N 147363 4207149', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': '17ttwdeYUrpVPPyja0eE5Hw2mjT6tp8SxPwIbla55CE=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': None, 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': None, 'v_countrycode': None, 'v_stateprovince': None, 'v_county': None, 'v_municipality': None, 'v_locality': None, 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '37.943527', 'v_decimallongitude': '-7.012607', 'v_geodeticdatum': None, 'v_coordinateuncertaintyinmeters': '1', 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': '30N 147363 4207149', 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': 37.943527, 'interpreted_decimallongitude': -7.012607, 'interpreted_countrycode': 'ES', 'occcount': 4, 'u_datumstr': None, 'tokens': 'es', 'matchme_with_coords': 'es30n147363420714937.943527-7.012607', 'matchme': 'es30n1473634207149', 'matchme_sans_coords': 'es', 'epsg': None, 'georef_score': 0, 'coordinates_score': 128, 'source': 'GBIF'}, {'dwc_location_hash': 'ndHZuXZ/HvXQpYZuqVleNb6AhompiDEdsrWjwM3C3DQ=', 'dwc:highergeographyid': '', 'dwc:highergeography': '', 'dwc:continent': 'Europe', 'dwc:waterbody': '', 'dwc:islandgroup': '', 'dwc:island': '', 'dwc:country': 'Sweden', 'dwc:countrycode': 'SE', 'dwc:stateprovince': '', 'dwc:county': 'Småland', 'dwc:municipality': 'Gislaved', 'dwc:locality': 'Kalvahagsviken, Mörke-Malen', 'dwc:verbatimlocality': '', 'dwc:minimumelevationinmeters': '', 'dwc:maximumelevationinmeters': '', 'dwc:verbatimelevation': '', 'dwc:minimumdepthinmeters': '', 'dwc:maximumdepthinmeters': '', 'dwc:verbatimdepth': '', 'dwc:minimumdistanceabovesurfaceinmeters': '', 'dwc:maximumdistanceabovesurfaceinmeters': '', 'dwc:locationaccordingto': '', 'dwc:locationremarks': '', 'dwc:decimallatitude': '57.3550320', 'dwc:decimallongitude': '13.4540679', 'dwc:geodeticdatum': '', 'dwc:coordinateuncertaintyinmeters': '10.00', 'dwc:coordinateprecision': '', 'dwc:pointradiusspatialfit': '', 'dwc:verbatimcoordinates': '', 'dwc:verbatimlatitude': '', 'dwc:verbatimlongitude': '', 'dwc:verbatimcoordinatesystem': '', 'dwc:verbatimsrs': '', 'dwc:footprintwkt': '', 'dwc:footprintsrs': '', 'dwc:footprintspatialfit': '', 'dwc:georeferencedby': '', 'dwc:georeferenceddate': '', 'dwc:georeferenceprotocol': '', 'dwc:georeferencesources': '', 'dwc:georeferenceverificationstatus': '', 'dwc:georeferenceremarks': '', 'locationid': 'ndHZuXZ/HvXQpYZuqVleNb6AhompiDEdsrWjwM3C3DQ=', 'v_highergeographyid': None, 'v_highergeography': None, 'v_continent': 'Europe', 'v_waterbody': None, 'v_islandgroup': None, 'v_island': None, 'v_country': 'Sweden', 'v_countrycode': 'SE', 'v_stateprovince': None, 'v_county': 'Småland', 'v_municipality': 'Gislaved', 'v_locality': 'Kalvahagsviken, Mörke-Malen', 'v_verbatimlocality': None, 'v_minimumelevationinmeters': None, 'v_maximumelevationinmeters': None, 'v_verbatimelevation': None, 'v_minimumdepthinmeters': None, 'v_maximumdepthinmeters': None, 'v_verbatimdepth': None, 'v_minimumdistanceabovesurfaceinmeters': None, 'v_maximumdistanceabovesurfaceinmeters': None, 'v_locationaccordingto': None, 'v_locationremarks': None, 'v_decimallatitude': '57.3550320', 'v_decimallongitude': '13.4540679', 'v_geodeticdatum': None, 'v_coordinateuncertaintyinmeters': '10.00', 'v_coordinateprecision': None, 'v_pointradiusspatialfit': None, 'v_verbatimcoordinates': None, 'v_verbatimlatitude': None, 'v_verbatimlongitude': None, 'v_verbatimcoordinatesystem': None, 'v_verbatimsrs': None, 'v_footprintwkt': None, 'v_footprintsrs': None, 'v_footprintspatialfit': None, 'v_georeferencedby': None, 'v_georeferenceddate': None, 'v_georeferenceprotocol': None, 'v_georeferencesources': None, 'v_georeferenceverificationstatus': None, 'v_georeferenceremarks': None, 'interpreted_decimallatitude': 57.355032, 'interpreted_decimallongitude': 13.454068, 'interpreted_countrycode': 'SE', 'occcount': 1, 'u_datumstr': None, 'tokens': 'europe se smaland gislaved kalvahagsviken morke malen se', 'matchme_with_coords': 'europesesmalandgislavedkalvahagsvikenmorkemalen57.35503213.4540679', 'matchme': 'europesesmalandgislavedkalvahagsvikenmorkemalen', 'matchme_sans_coords': 'europesesmalandgislavedkalvahagsvikenmorkemalen', 'epsg': None, 'georef_score': 0, 'coordinates_score': 128, 'source': 'GBIF'}]
        self.assertEqual(result, target)