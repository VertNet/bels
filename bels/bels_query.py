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
__filename__ = "bels_query.py"
__version__ = __filename__ + ' ' + "2022-06-19T14:58-03:00"

import json
import logging
import re
from google.cloud import bigquery
from google.cloud import storage

from json_utils import CustomJsonEncoder
from dwca_utils import lower_dict_keys

BQ_SERVICE='localityservice'
BQ_GAZ_DATASET='gazetteer'
BQ_VOCABS_DATASET='vocabs'
BQ_PROJECT='localityservice'
BQ_INPUT_DATASET='belsapi'
BQ_OUTPUT_DATASET='results'

def georeference_score(locdict):
    # Assumes the locdict has been darwinized
    if locdict is None:
        return None
    lowerlocdict = lower_dict_keys(locdict)
    georeferenceprotocol = lowerlocdict.get('georeferenceprotocol')
    georeferencesources = lowerlocdict.get('georeferencesources')
    georeferenceddate = lowerlocdict.get('georeferenceddate')
    georeferencedby = lowerlocdict.get('georeferencedby')
    georeferenceremarks = lowerlocdict.get('georeferenceremarks')
    
    score = 0
    if georeferenceprotocol is not None and len(georeferenceprotocol.strip())>0:
        score += 16
    if georeferencesources is not None and len(georeferencesources.strip())>0:
        score += 8
    if georeferenceddate is not None and len(georeferenceddate.strip())>0:
        score += 4
    if georeferencedby is not None and len(georeferencedby.strip())>0:
        score += 2
    if georeferenceremarks is not None and len(georeferenceremarks.strip())>0:
        score += 1
    return score

def coordinates_score(locdict):
    # Assumes the locdict has been darwinized
    if locdict is None:
        return None
    lowerlocdict = lower_dict_keys(locdict)
    decimallatitude = lowerlocdict.get('decimallatitude')
    decimallongitude = lowerlocdict.get('decimallongitude')
    geodeticdatum = lowerlocdict.get('geodeticdatum')
    coordinateuncertaintyinmeters = lowerlocdict.get('coordinateuncertaintyinmeters')
    
    score = 0
    try:
        lat = float(decimallatitude)
        lng = float(decimallongitude)
        if lat>=-90 and lat <=90 and lng>=-180 and lng<=180:
            score += 128
    except Exception as e:
        pass
    if geodeticdatum is not None:
        score += 64
    try:
        unc = float(coordinateuncertaintyinmeters)
        if unc>=1 and unc<=20037509:
            score += 32
    except Exception as e:
        pass
    score += georeference_score(locdict)
    return score

def has_georef(locdict):
    if locdict is None:
         return None
    if coordinates_score(locdict)>=224:
        return True
    return False

def has_decimal_coords(locdict):
    if locdict is None:
         return None
    if coordinates_score(locdict)>=128:
        return True
    return False

def has_verbatim_coords(locdict):
    if locdict is None:
         return None
    if (locdict.get('verbatimlatitude') is not None and \
        locdict.get('verbatimlongitude') is not None) or \
        locdict.get('verbatimcoordinates') is not None:
        return True
    return False

def bels_original_georef(locdict):
    if locdict is None:
         return None
    row = {}
    row['bels_countrycode'] = None
    row['bels_match_string'] = None
    row['bels_decimallatitude'] = locdict.get('decimallatitude')
    row['bels_decimallongitude'] = locdict.get('decimallongitude')
    row['bels_geodeticdatum'] = locdict.get('geodeticdatum')
    row['bels_coordinateuncertaintyinmeters'] = locdict.get('coordinateuncertaintyinmeters')
    row['bels_georeferencedby'] = locdict.get('georeferencedby')
    row['bels_georeferenceddate'] = locdict.get('georeferenceddate')
    row['bels_georeferenceprotocol'] = locdict.get('georeferenceprotocol')
    row['bels_georeferencesources'] = locdict.get('georeferencesources')
    row['bels_georeferenceremarks'] = locdict.get('georeferenceremarks')
    row['bels_georeference_score'] = georeference_score(locdict)
    row['bels_georeference_source'] = 'original data'
    row['bels_best_of_n_georeferences'] = 1
    row['bels_match_type'] = 'original georeference'
    return row

class BELS_Client():
    def __init__(self, bq_client=None):
        self.bq_client = bq_client;
        if self.bq_client is None:
            print(f'New BigQuery Client instantiated for {self.__class__.__name__}')
            self.bq_client = bigquery.Client()
        self.countrycode_dict = {}

    def get_bq_client(self):
        return self.bq_client

    def populate(self, table_id=None):
        self.countrycode_dict = {}
        if table_id is None:
            table_id = BQ_SERVICE+'.'+BQ_VOCABS_DATASET+'.'+'countrycode_lookup'
        query =f"""
SELECT 
    *
FROM 
    {table_id}
"""
        table = self.bq_client.get_table(table_id)  # Make an API request.
        rowlist = list(self.bq_client.list_rows(table))
        for row in rowlist:
            self.countrycode_dict[row['u_country']]=row['countrycode']

    def country_report(self, count=0):
        if self.countrycode_dict is None:
            print(f"Countrycode dictionary not populated.")
            return

        print(f"Loaded {len(self.countrycode_dict)} rows for countrycode lookup.")
        if count>0:
            counter = 0
            print(f'First {count} countrycode lookups:')
            for key in self.countrycode_dict:
                print(f'{key}: {self.countrycode_dict[key]}')
                counter += 1
                if counter == count:
                    break
        else:
            print(self.countrycode_dict)

    def get_best_countrycode(self, locdict):
#        print(f'locdict:{locdict}')
        if locdict is None:
            return None
        best_country = locdict.get('interpreted_countrycode')
        if best_country is None:
            best_country = locdict.get('v_countrycode')
        if best_country is None:
            best_country = locdict.get('country')
        if best_country is not None:
#            print(f'bestcountry:{best_country}')
            countrycode = self.countrycode_dict.get(best_country.upper())
#            print(f'bestcountrycode:{countrycode}')
            return countrycode
        return None

def schema_from_header(header):
    # Create a BigQuery schema from the fields in a header.
    schema = []
    for field in header:
        schema.append(bigquery.SchemaField(field, "STRING"))
    return schema

def bigquerify_header(header):
    # The rules for field names in BigQuery are:
    #   - start with a letter or underscore
    #   - contain only letters, numbers, and underscores, 
    #   - be at most 128 characters long
    #   - can't be blank
    # In addition, we don't want any duplicated field names.
    bigqueryized_header = []
    i = 1
    for f in header:
        if f == '' or f is None:
            f = str(i)
        if f[0] != '_' and f[0].isalpha()==False:
            f = '_' + f
        f = re.sub(r'[^\w_]','_',f)
        while f in bigqueryized_header:
            f = '_' + f
        f = f[0:127]
        bigqueryized_header.append(f)
        i += 1
    return bigqueryized_header

def country_fields(header):
    # Get a list containing the fields that can be used to interpret the countrycode in
    # order of priority: interpreted_countrycode, countrycode, v_countrycode, country
    # Depends on header being Darwinized as lower case.
    countryfieldslist = ['interpreted_countrycode', 'countrycode', 'v_countrycode', 
        'country']
    countryfieldsfound = []
    for countryfield in countryfieldslist:
        if countryfield in header:
            countryfieldsfound.append(countryfield)
    if len(countryfieldsfound) == 0:
        return None
    return countryfieldsfound

def process_import_table(bq_client, input_table_id, countryfieldlist):
    if input_table_id is None or bq_client is None:
        return None
    if countryfieldlist is None or len(countryfieldlist) == 0:
        return None
    table_parts = input_table_id.split('.')
    table_name = table_parts[len(table_parts)-1]
    output_table_id = BQ_PROJECT+'.'+BQ_OUTPUT_DATASET+'.'+table_name
    
    matchcountryclause = None
    
    if 'interpreted_countrycode' in countryfieldlist:
        # ['interpreted_countrycode',...]
        if 'countrycode' in countryfieldlist:
            # ['interpreted_countrycode','countrycode',...]
            if 'v_countrycode' in countryfieldlist:
                # ['interpreted_countrycode','countrycode','v_countrycode',...]
                if 'country' in countryfieldlist:
                    # ['interpreted_countrycode','countrycode','v_countrycode','country']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,IF(countrycode IS NULL,IF(v_countrycode IS NULL,country,v_countrycode),countrycode),interpreted_countrycode) AS bels_match_country
                    """
                else:
                    # ['interpreted_countrycode','countrycode','v_countrycode']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,IF(countrycode IS NULL,v_countrycode,countrycode),interpreted_countrycode) AS bels_match_country
                    """
            else:
                # ['interpreted_countrycode','countrycode',...]
                # no v_countrycode
                if 'country' in countryfieldlist:
                    # ['interpreted_countrycode','countrycode','country']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,IF(countrycode IS NULL,country,countrycode),interpreted_countrycode) AS bels_match_country
                    """
                else:
                    # ['interpreted_countrycode','countrycode']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,countrycode,interpreted_countrycode) AS bels_match_country
                    """
        else:
            # ['interpreted_countrycode',...]
            # no countrycode
            if 'v_countrycode' in countryfieldlist:
                # ['interpreted_countrycode','v_countrycode',...]
                if 'country' in countryfieldlist:
                    # ['interpreted_countrycode','v_countrycode','country']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,IF(v_countrycode IS NULL,country,v_countrycode),interpreted_countrycode) AS bels_match_country
                    """
                else:
                    # ['interpreted_countrycode','v_countrycode']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,v_countrycode,interpreted_countrycode) AS bels_match_country
                    """
            else:
                # ['interpreted_countrycode',...]
                # no countrycode, no v_countrycode
                if 'country' in countryfieldlist:
                    # ['interpreted_countrycode','country']
                    matchcountryclause = \
                    """
                    IF(interpreted_countrycode IS NULL,country,interpreted_countrycode) AS bels_match_country
                    """
                else:
                    # ['interpreted_countrycode']
                    matchcountryclause = \
                    """
                    interpreted_countrycode AS bels_match_country
                    """
    else:
        # no interpreted_countrycode
        if 'countrycode' in countryfieldlist:
            # ['countrycode',...]
            if 'v_countrycode' in countryfieldlist:
                # ['countrycode','v_countrycode',...]
                if 'country' in countryfieldlist:
                    # ['countrycode','v_countrycode','country']
                    matchcountryclause = \
                    """
                    IF(countrycode IS NULL,IF(v_countrycode IS NULL,country,v_countrycode),countrycode) AS bels_match_country
                    """
                else:
                    # ['countrycode','v_countrycode']
                    matchcountryclause = \
                    """
                    IF(countrycode IS NULL,v_countrycode,countrycode) AS bels_match_country
                    """
            else:
                # ['countrycode',...]
                # no interpreted_countrycode, no v_countrycode
                if 'country' in countryfieldlist:
                    # ['countrycode','country']
                    matchcountryclause = \
                    """
                    IF(countrycode IS NULL,country,countrycode) AS bels_match_country
                    """
                else:
                    # ['countrycode']
                    matchcountryclause = \
                    """
                    countrycode AS bels_match_country
                    """
        else:
            # no interpreted_countrycode, no countrycode
            if 'v_countrycode' in countryfieldlist:
                # ['v_countrycode',...]
                if 'country' in countryfieldlist:
                    # ['v_countrycode','country']
                    matchcountryclause = \
                    """
                    IF(v_countrycode IS NULL,country,v_countrycode) AS bels_match_country
                    """
                else:
                    # ['v_countrycode']
                    matchcountryclause = \
                    """
                    v_countrycode AS bels_match_country
                    """
            else:
                # no interpreted-countrycode, no countrycode, no v_countrycode
                if 'country' in countryfieldlist:
                    # ['country']
                    matchcountryclause = \
                    """
                    country AS bels_match_country
                    """
                else:
                    # no interpretable country fields
                    return None
        
    # Script georeference matching in BigQuery
    query = \
f"""
-- Georeference matching script. Input table is expected to have Location column 
-- names mapped to Darwin Core Location term names with at least one interpretable
-- country field from among the set: 'interpreted-countrycode', 'countrycode', 
-- 'v_countrycode','country'
BEGIN
-- Make a table adding a field bels_match_country to the input table, mapping the best 
-- existing matching country field in the input.
CREATE TEMP TABLE countrify AS (
  SELECT 
    *,
    {matchcountryclause}
  FROM 
    `{input_table_id}`);

CREATE TEMP TABLE interpreted AS (
SELECT 
  a.*,
  b.countrycode AS bels_interpreted_countrycode, 
  GENERATE_UUID() AS bels_id
FROM 
  countrify a 
LEFT JOIN
  `localityservice.vocabs.countrycode_lookup` b 
ON 
  UPPER(a.bels_match_country)=b.u_country
);

-- Make the match strings
CREATE TEMP TABLE matcher AS (
SELECT
  bels_id,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(functions.matchString(TO_JSON_STRING(t), "withcoords"))),NFKC)),r"[\s]+",'') AS bels_matchwithcoords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(functions.matchString(TO_JSON_STRING(t), "verbatimcoords"))),NFKC)),r"[\s]+",'') AS bels_matchverbatimcoords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(functions.matchString(TO_JSON_STRING(t), "sanscoords"))),NFKC)),r"[\s]+",'') AS bels_matchsanscoords
FROM 
  interpreted AS t
);

-- CREATE table georefs from matchme_with_coords
CREATE TEMP TABLE georefs AS (
SELECT
  a.bels_id,
  interpreted_decimallatitude AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  IF(interpreted_decimallatitude IS NULL,NULL,'epsg:4326') AS bels_geodeticdatum,
  SAFE_CAST(round(unc_numeric,0) AS INT64) AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby AS bels_georeferencedby,
  v_georeferenceddate AS bels_georeferenceddate,
  v_georeferenceprotocol AS bels_georeferenceprotocol,
  v_georeferencesources AS bels_georeferencesources,
  v_georeferenceremarks AS bels_georeferenceremarks,
  georef_score AS bels_georeference_score,
  source AS bels_georeference_source,
  georef_count AS bels_best_of_n_georeferences,
  'match using coords' AS bels_match_type
FROM
  matcher a,
  `localityservice.gazetteer.matchme_with_coords_best_georef` b
WHERE
  a.bels_matchwithcoords=b.matchme_with_coords
);

-- APPEND verbatim coords matches to georefs
INSERT INTO georefs (
SELECT
  a.bels_id,
  interpreted_decimallatitude AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  IF(interpreted_decimallatitude IS NULL,NULL,'epsg:4326') AS bels_geodeticdatum,
  SAFE_CAST(round(unc_numeric,0) AS INT64) AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby AS bels_georeferencedby,
  v_georeferenceddate AS bels_georeferenceddate,
  v_georeferenceprotocol AS bels_georeferenceprotocol,
  v_georeferencesources AS bels_georeferencesources,
  v_georeferenceremarks AS bels_georeferenceremarks,
  georef_score AS bels_georeference_score,
  source AS bels_georeference_source,
  georef_count AS bels_best_of_n_georeferences,
  'match using verbatim coords' AS bels_match_type
FROM
  matcher a,
  `localityservice.gazetteer.matchme_verbatimcoords_best_georef` b
WHERE
  a.bels_matchverbatimcoords=b.matchme AND
  a.bels_id NOT IN (
SELECT 
  bels_id
FROM georefs
)
);

-- APPEND sans coords matches to georefs
INSERT INTO georefs (
SELECT
  a.bels_id,
  interpreted_decimallatitude AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  IF(interpreted_decimallatitude IS NULL,NULL,'epsg:4326') AS bels_geodeticdatum,
  SAFE_CAST(round(unc_numeric,0) AS INT64) AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby AS bels_georeferencedby,
  v_georeferenceddate AS bels_georeferenceddate,
  v_georeferenceprotocol AS bels_georeferenceprotocol,
  v_georeferencesources AS bels_georeferencesources,
  v_georeferenceremarks AS bels_georeferenceremarks,
  georef_score AS bels_georeference_score,
  source AS bels_georeference_source,
  georef_count AS bels_best_of_n_georeferences,
  'match sans coords' AS bels_match_type
FROM
  matcher a,
  `localityservice.gazetteer.matchme_sans_coords_best_georef` b
WHERE
  a.bels_matchsanscoords=b.matchme_sans_coords AND
  a.bels_id NOT IN (
SELECT 
  bels_id
FROM georefs
)
);

-- Add georefs to original data as results
CREATE OR REPLACE TABLE `{output_table_id}`
AS
SELECT
  a.* EXCEPT (bels_id),
  b.bels_matchwithcoords,
  b.bels_matchverbatimcoords,
  b.bels_matchsanscoords,
  c.* EXCEPT (bels_id)
FROM
  interpreted a
JOIN matcher b ON a.bels_id=b.bels_id
LEFT JOIN georefs c ON b.bels_id=c.bels_id;
END;
"""

    # Make a BigQuery API job request.
    query_job = bq_client.query(query)

    # Wait for the job to complete. result is a RowIterator, which is actually not an
    # Iterator, but rather an Iterable. This, to iterate over it, use iter(result)
    result = query_job.result()  

    # The following may be useful if it is desired to bypass saving a persistent table - 
    # to export the destination table to Google Cloud Storage directly. For testing, for 
    # now, we'll save the table in BQ_OUTPUT_DATASET

    # All queries write to a destination table. If a destination table is not specified, 
    # BigQuery populates it with a reference to a temporary anonymous table after the 
    # query completes.

    # Get the reference to the destination table for the query results.
#    destination = query_job.destination

    # Get the Table object from the destination table reference.
#    destination = bq_client.get_table(destination)

    if isinstance(result, bigquery.table._EmptyRowIterator) == False:
        logging.debug('Correctly detected _EmptyRowIterator.')
        return output_table_id
    return output_table_id

def import_table(bq_client, gcs_uri, header, table_name=None):
    # Create a table in BigQuery from a file in Google Cloud Storage with the given
    # header.
    #  Example: gcs_uri = "gs://localityservice/jobs/test_matchme_sans_coords_best_georef.csv"

    if gcs_uri is None:
        logging.error('No Google Cloud Storage location provided.')
        return None
    if header is None:
        logging.error('No file header provided.')
        return None

    # Set table_id to the ID of the table to create.
    if table_name is None:
        uri_parts = gcs_uri.split('/')
        file_name = uri_parts[len(uri_parts)-1]
        file_parts = file_name.split('.')
        table_name = file_parts[0]
    table_id = BQ_PROJECT+'.'+BQ_INPUT_DATASET+'.'+table_name

    schema = schema_from_header(header)
    logging.debug(f'Header: {header}\nSchema: {schema}')

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        allow_quoted_newlines=True,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV
   )

    # First delete the table if it already exists
    try:
        bq_client.get_table(table_id)
        delete_table(bq_client, table_id)
        try:
            bq_client.get_table(table_id)
        except Exception as e:
            logging.debug(f'Table {table_id} was deleted.')
    except Exception as e:
        logging.debug(f'Table {table_id} does not exist.')

    output_table_id = BQ_PROJECT+'.'+BQ_OUTPUT_DATASET+'.'+table_name
    try:
        bq_client.get_table(output_table_id)
        delete_table(bq_client, output_table_id)
        try:
            bq_client.get_table(output_table_id)
        except Exception as e:
            logging.debug(f'Table {output_table_id} was deleted.')
    except Exception as e:
        logging.debug(f'Table {output_table_id} does not exist.')

    try:
        # Load the table from Google Cloud Storage to the identified table
        load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        # Wait for the job to complete.
        load_job.result()
    except Exception as e:
        logging.error(f'Unable to load {gcs_uri} to {table_id}. {e}')

    destination_table = None
    try:
        destination_table = bq_client.get_table(table_id)
    except Exception as e:
        logging.error(f'Table {table_id} was not created.')
        return None
    logging.debug(f'Loaded {destination_table.num_rows} rows from {gcs_uri} into {table_id}.')
    # Success. Return the full table identifier.
    return table_id

def delete_table(bq_client, table_id):
    bq_client.delete_table(table_id, not_found_ok=True)
    # delete_table has no return value

def export_table(bq_client, table_id, destination_uri):
    # From https://cloud.google.com/bigquery/docs/exporting-data
    logging.debug(f'table_id: {table_id} destination_uri: {destination_uri}')
    job_config = bigquery.job.ExtractJobConfig()
    job_config.compression = 'GZIP'
    extract_job = bq_client.extract_table(
        table_id,
#        table_ref,
        destination_uri,
        job_config = job_config,
        # Location must match that of the source table.
#        location="US",
    ) # API request
    # Wait for job to complete.
    extract_job.result()

    storage_client = storage.Client()
    locparts = destination_uri.split('/')
    bucket = storage_client.get_bucket(locparts[2])
    blobname = destination_uri[destination_uri.find(locparts[3]):]
    bloblist = []
#    if blob.exists == False or blob.size == 0:
    if bucket.get_blob(blobname) is None:
        # The blob did not get written. It is likely that this is because it exceeded the
        # size limit for an export to a single file. Change the destination_uri to 
        # include a wildcard and try again.
        destination_uri = destination_uri.split('.csv.gz')[0]+'*.csv.gz'
        extract_job = bq_client.extract_table(
            table_id,
#            table_ref,
            destination_uri,
            job_config = job_config,
            # Location must match that of the source table.
#            location="US",
        ) # API request
        # Wait for job to complete.
        extract_job.result()
        blobpattern = destination_uri[destination_uri.find(locparts[3]):].split('*.csv.gz')[0]+'-0'
        logging.info(f'blob pattern: {blobpattern}')
        for blob in bucket.list_blobs():
            logging.info(f'blob name: {blob.name}')
            if blob.name.find(blobpattern) != -1:
                bloblist.append(blob.name)
    else:
        bloblist.append(blobname)
    return bloblist

def query_location_by_id(base64locationhash, table_name=None):
    ''' Create a query string to get a location record from the distinct Locations data
        store in BigQuery using the BASE64 representation of the location identifier.
    parameters:
        base64locationhash - string representation of the base64 digest of the sha256
            location hash (what humans see in the BigQuery UI as dwc_location_hash).
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_location_by_id()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'locations_distinct_with_scores'
    query =f"""
SELECT 
    TO_BASE64(dwc_location_hash) as locationid, *
FROM 
    {table_name}
WHERE 
    TO_BASE64(dwc_location_hash)='{base64locationhash}'
"""
    return query

def query_location_by_hashid(locationhash, table_name=None):
    ''' Create a query string to get a location record from the distinct Locations data
        store in BigQuery using the binary hash representation of the identifier.
    parameters:
        locationhash - binary representation of the sha256
            location hash (not what humans see in the BigQuery UI as dwc_location_hash).
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_location_by_hashid()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'locations_distinct_with_scores'
    query =f"""
SELECT 
    dwc_location_hash as locationid, *
FROM
    {table_name}
WHERE 
    dwc_location_hash={locationhash}
"""
    return query

def get_location_by_id(bq_client, base64locationhash):
    ''' Get the first row from query_location_by_id().
    parameters:
        bq_client - an instance of a bigquery.Client().
        base64locationhash - string representation of the base64 digest of the sha256
            location hash (what humans see in the BigQuery UI as dwc_location_hash).
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_location_by_id()'

    rows = run_bq_query(bq_client, query_location_by_id(base64locationhash), 1)
    for row in rows:
        return row_as_dict(row)

def get_location_by_hashid(bq_client, locationhash):
    ''' Get the first row from query_location_by_hashid().
    parameters:
        bq_client - an instance of a bigquery.Client().
        locationhash - binary representation of the sha256 location hash (not what humans 
            see in the BigQuery UI as dwc_location_hash).
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_location_by_hashid()'

    rows = run_bq_query(bq_client, query_location_by_hashid(locationhash), 1)
    for row in rows:
        return row_as_dict(row)

def query_best_sans_coords_georef(matchstr, table_name=None):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the part of the 
        Location without coordinates from the Locations data store in BigQuery.
    parameters:
        matchstr - the matchme_sans_coords string to match.
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_best_sans_coords_georef()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'matchme_sans_coords_best_georef'
    query =f"""
SELECT *
FROM 
    {table_name}
WHERE 
    matchme_sans_coords='{matchstr}'
"""
    return query

def query_best_sans_coords_georef_reduced(matchstr, table_name=None):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the part of the 
        Location without coordinates from the Locations data store in BigQuery. Return 
        only fields needed to append for a download.
    parameters:
        matchstr - the matchme_sans_coords string to match.
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_best_sans_coords_georef_reduced()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'matchme_sans_coords_best_georef'
    query =f"""
SELECT 
  interpreted_countrycode as bels_countrycode,
  matchme_sans_coords as bels_match_string,
  interpreted_decimallatitude as bels_decimallatitude,
  interpreted_decimallongitude as bels_decimallongitude,
  'epsg:4326' as bels_geodeticdatum,
  SAFE_CAST(round(unc_numeric,0) AS INT64) AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby as bels_georeferencedby,
  v_georeferenceddate as bels_georeferenceddate,
  v_georeferenceprotocol as bels_georeferenceprotocol,
  v_georeferencesources as bels_georeferencesources,
  v_georeferenceremarks as bels_georeferenceremarks,
  georef_score as bels_georeference_score,
  source as bels_georeference_source,
  georef_count as bels_best_of_n_georeferences,
  'match sans coords' as bels_match_type
FROM 
    {table_name}
WHERE 
    matchme_sans_coords='{matchstr}'
"""
    return query

def get_best_sans_coords_georef(bq_client, matchstr):
    ''' Get the first row from query_best_sans_coords_georef().
    parameters:
        bq_client - an instance of a bigquery.Client().
        matchstr - the matchme_sans_coords string to match.
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_best_sans_coords_georef()'

    rows = run_bq_query(bq_client, query_best_sans_coords_georef(matchstr), 1)
    if rows.total_rows==0:
        # Create a dict of an empty row so that every record can have a result
        # This has to match the structure of the rows query result.        
        return None
#         return {
#         'sans_coords_match_string': None, 
#         'sans_coords_countrycode': None, 
#         'sans_coords_decimallatitude': None, 
#         'sans_coords_decimallongitude': None, 
#         'sans_coords_coordinateuncertaintyinmeters': None, 
#         'sans_coords_georeferencedby': None, 
#         'sans_coords_georeferenceddate': None, 
#         'sans_coords_georeferenceprotocol': None, 
#         'sans_coords_georeferencesources': None, 
#         'sans_coords_georeferenceremarks': None, 
#         'sans_coords_georef_score': None, 
#         'sans_coords_centroid_distanceinmeters': None, 
#         'sans_coords_georef_count': None }
    for row in rows:
        return row_as_dict(row)

def get_best_sans_coords_georef_reduced(bq_client, matchstr):
    ''' Get the first row from query_best_sans_coords_georef_reduced().
    parameters:
        bq_client - an instance of a bigquery.Client().
        matchstr - the matchme_sans_coords string to match.
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_best_sans_coords_georef_reduced()'
    query = query_best_sans_coords_georef_reduced(matchstr)
    rows = run_bq_query(bq_client, query, 1)
    # logging.debug(f'{__version__} query: {query} row count: {rows.total_rows}')
    if rows.total_rows==0:
        # Create a dict of an empty row so that every record can have a result
        # This has to match the structure of the rows query result.
        return None
#         return {
#         'sans_coords_match_string': None, 
#         'sans_coords_countrycode': None, 
#         'sans_coords_decimallatitude': None, 
#         'sans_coords_decimallongitude': None, 
#         'sans_coords_coordinateuncertaintyinmeters': None, 
#         'sans_coords_georeferencedby': None, 
#         'sans_coords_georeferenceddate': None, 
#         'sans_coords_georeferenceprotocol': None, 
#         'sans_coords_georeferencesources': None, 
#         'sans_coords_georeferenceremarks': None, 
#         'sans_coords_georef_score': None, 
#         'sans_coords_centroid_distanceinmeters': None, 
#         'sans_coords_georef_count': None }
    for row in rows:
        return row_as_dict(row)

def query_best_with_verbatim_coords_georef(matchstr, table_name=None):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the part of the 
        Location without coordinates, but with verbatim coordinates, from the Locations 
        data store in BigQuery.
    parameters:
        matchstr - the matchme string to match.
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_best_with_verbatim_coords_georef()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'matchme_verbatimcoords_best_georef'
    query =f"""
SELECT *
FROM 
    {table_name}
WHERE 
    matchme='{matchstr}'
        """
    return query

def query_best_with_verbatim_coords_georef_reduced(matchstr, table_name=None):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the part of the 
        Location without coordinates, but with verbatim coordinates, from the Locations 
        data store in BigQuery. Return only fields needed to append for a download.
    parameters:
        matchstr - the matchme string to match.
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_best_with_verbatim_coords_georef_reduced()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'matchme_verbatimcoords_best_georef'
    query =f"""
SELECT 
  interpreted_countrycode as bels_countrycode,
  matchme as bels_match_string,
  interpreted_decimallatitude as bels_decimallatitude,
  interpreted_decimallongitude as bels_decimallongitude,
  'epsg:4326' as bels_geodeticdatum,
  SAFE_CAST(round(unc_numeric,0) AS INT64) AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby as bels_georeferencedby,
  v_georeferenceddate as bels_georeferenceddate,
  v_georeferenceprotocol as bels_georeferenceprotocol,
  v_georeferencesources as bels_georeferencesources,
  v_georeferenceremarks as bels_georeferenceremarks,
  georef_score as bels_georeference_score,
  source as bels_georeference_source,
  georef_count as bels_best_of_n_georeferences,
  'match using verbatim coords' as bels_match_type
FROM 
  {table_name}
WHERE 
  matchme='{matchstr}'
"""
    return query

def get_best_with_verbatim_coords_georef(bq_client, matchstr):
    ''' Get the first row from query_best_with_verbatim_coords_georef().
    parameters:
        bq_client - an instance of a bigquery.Client().
        matchstr - the matchme string to match.
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_best_sans_coords_georef()'

    rows = run_bq_query(bq_client, query_best_with_verbatim_coords_georef(matchstr), 1)
    for row in rows:
        return row_as_dict(row)

def get_best_with_verbatim_coords_georef_reduced(bq_client, matchstr):
    ''' Get the first row from query_best_with_verbatim_coords_georef_reduced().
    parameters:
        bq_client - an instance of a bigquery.Client().
        matchstr - the matchme string to match.
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_best_sans_coords_georef_reduced()'

    rows = run_bq_query(bq_client, query_best_with_verbatim_coords_georef_reduced(matchstr), 1)
    if rows.total_rows==0:
#        # Create a dict of an empty row so that every record can have a result
#        # This has to match the structure of the rows query result.
        return None
#         return {
#         'sans_coords_match_string': None, 
#         'sans_coords_countrycode': None, 
#         'sans_coords_decimallatitude': None, 
#         'sans_coords_decimallongitude': None, 
#         'sans_coords_coordinateuncertaintyinmeters': None, 
#         'sans_coords_georeferencedby': None, 
#         'sans_coords_georeferenceddate': None, 
#         'sans_coords_georeferenceprotocol': None, 
#         'sans_coords_georeferencesources': None, 
#         'sans_coords_georeferenceremarks': None, 
#         'sans_coords_georef_score': None, 
#         'sans_coords_centroid_distanceinmeters': None, 
#         'sans_coords_georef_count': None }
    for row in rows:
        return row_as_dict(row)

def query_best_with_coords_georef(matchstr, table_name=None):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the whole 
        Location, with coordinates, from the Locations data store in BigQuery.
    parameters:
        matchstr - the matchme_with_coords string to match.
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_best_with_coords_georef()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'matchme_with_coords_best_georef'
    query =f"""
SELECT *
FROM 
    {table_name}
WHERE 
    matchme_with_coords='{matchstr}'
"""
    return query

def query_best_with_coords_georef_reduced(matchstr, table_name=None):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the whole 
        Location, with coordinates, from the Locations data store in BigQuery. Return 
        only fields needed to append for a download.
    parameters:
        matchstr - the matchme_with_coords string to match.
        table_name - full table name on which the query should be based.
    returns:
        query - the query string
    '''
    functionname = 'query_best_with_coords_georef_reduced()'

    if table_name is None:
        table_name = BQ_SERVICE+'.'+BQ_GAZ_DATASET+'.'+'matchme_with_coords_best_georef'
    query =f"""
SELECT 
  interpreted_countrycode as bels_countrycode,
  matchme_with_coords as bels_match_string,
  interpreted_decimallatitude as bels_decimallatitude,
  interpreted_decimallongitude as bels_decimallongitude,
  'epsg:4326' as bels_geodeticdatum,
  SAFE_CAST(round(unc_numeric,0) AS INT64) AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby as bels_georeferencedby,
  v_georeferenceddate as bels_georeferenceddate,
  v_georeferenceprotocol as bels_georeferenceprotocol,
  v_georeferencesources as bels_georeferencesources,
  v_georeferenceremarks as bels_georeferenceremarks,
  georef_score as bels_georeference_score,
  source as bels_georeference_source,
  georef_count as bels_best_of_n_georeferences,
  'match with coords' as bels_match_type
FROM 
  {table_name}
WHERE 
  matchme_with_coords='{matchstr}'
"""
    return query

def get_best_with_coords_georef(bq_client, matchstr):
    ''' Get the first row from query_best_with_coords_georef().
    parameters:
        bq_client - an instance of a bigquery.Client().
        matchstr - the matchme_with_coords string to match.
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_best_with_coords_georef()'

    rows = run_bq_query(bq_client, query_best_with_coords_georef(matchstr), 1)
    for row in rows:
        return row_as_dict(row)

def get_best_with_coords_georef_reduced(bq_client, matchstr):
    ''' Get the first row from query_best_with_coords_georef_reduced().
    parameters:
        bq_client - an instance of a bigquery.Client().
        matchstr - the matchme_with_coords string to match.
    returns:
        row_as_dict(row) - the first row of the query result as a dict.
    '''
    functionname = 'get_best_with_coords_georef_reduced()'

    rows = run_bq_query(bq_client, query_best_with_coords_georef_reduced(matchstr), 1)
    if rows.total_rows==0:
        return None
#         # Create a dict of an empty row so that every record can have a result
#         # This has to match the structure of the rows query result.
#         return {
#         'sans_coords_match_string': None, 
#         'sans_coords_countrycode': None, 
#         'sans_coords_decimallatitude': None, 
#         'sans_coords_decimallongitude': None, 
#         'sans_coords_coordinateuncertaintyinmeters': None, 
#         'sans_coords_georeferencedby': None, 
#         'sans_coords_georeferenceddate': None, 
#         'sans_coords_georeferenceprotocol': None, 
#         'sans_coords_georeferencesources': None, 
#         'sans_coords_georeferenceremarks': None, 
#         'sans_coords_georef_score': None, 
#         'sans_coords_centroid_distanceinmeters': None, 
#         'sans_coords_georef_count': None }
    for row in rows:
        return row_as_dict(row)

def run_bq_query(bq_client, querystr, max_results):
    ''' Execute a query through a bigquery.Client().
    parameters:
        bq_client - an instance of a bigquery.Client().
        querystr - the matchme_with_coords string to match.
        max_results - an upper limit on the number of rows returned.
    returns:
        rows - an Iterable containing  rows from the query result. A row can be turned 
            into a list, with row_as_list(), or into a dict, with row_as_dict().
    '''
    functionname = 'run_bq_query()'

    query_job = bq_client.query(querystr)  # Make a BigQuery API job request.
    query_job.result()  # Wait for the job to complete.

    # All queries write to a destination table. If a destination table is not specified, 
    # BigQuery populates it with a reference to a temporary anonymous table after the 
    # query completes.

    # Get the reference to the destination table for the query results.
    destination = query_job.destination

    # Get the Table object from the destination table reference.
    destination = bq_client.get_table(destination)
    # TODO: Is the following necessary? query_job.result() returns a row Iterable
    # See https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJob.html#google.cloud.bigquery.job.QueryJob.result
    
    # Get a RowIterator with the results.
    # Note: a google.cloud.bigquery.table.RowIterator is not actually an Iterator, 
    # but rather an Iterable. It can be iterated over with iter(result) to get rows, but 
    # don't expect full Iterator behavior.
    rows = bq_client.list_rows(destination, max_results=max_results)
    return rows

def row_as_list(row):
    ''' Get row data as a list.
    parameters:
        row - an instance of row data from a table.
    returns:
        row_list - the row as a list of (key, value) tuples.
    '''
    functionname = 'row_as_list()'

    row_list = list(row.items())
    return row_list

def row_as_dict(row):
    ''' Get row data as a dict.
    parameters:
        row - an instance of row data from a table.
    returns:
        row_dict - the row as a dict.
    '''
    functionname = 'row_as_dict()'

    row_dict = {}
    for item in row_as_list(row):
        row_dict[item[0]]=item[1]
    return row_dict

def row_as_json(row):
    ''' Get row data as JSON. Uses a Custom Encoder to make sure non-JSON-able entities
        (e.g., bytes, Decimals) can be rendered.
    parameters:
        row - an instance of row data from a table.
    returns:
        row_json - the row as JSON.
    '''
    functionname = 'row_as_json()'

    row_json=json.dumps(row_as_dict(row), cls=CustomJsonEncoder)
    return row_json
    
# def main():
#     BQ = bigquery.Client()
#     matchstr = 'auwac73kmsofbillabongroadhouse'
#     result = get_best_sans_coords_georef(BQ, matchstr)
#     print(result)
# 
#     matchstr = 'aqbechervaiseisland00-66.49559.49'
#     result = get_best_with_coords_georef(BQ, matchstr)
#     print(result)
# 
#     matchstr = 'usminnesotawadenat136nr33ws.1012-jul-71,'
#     result = get_best_with_verbatim_coords_georef(BQ, matchstr)
#     print(result)
# 
# if __name__ == '__main__':
#     main()
