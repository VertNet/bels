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
__filename__ = "bels_query.py"
__version__ = __filename__ + ' ' + "2021-07-24T18:38-03:00"

import json
import logging
import re
from google.cloud import bigquery
from google.cloud import storage
from .dwca_terms import locationkeytermlist
from .json_utils import CustomJsonEncoder

BQ_SERVICE='localityservice'
BQ_GAZ_DATASET='gazetteer'
BQ_PROJECT='localityservice'
BQ_INPUT_DATASET='belsapi'
BQ_OUTPUT_DATASET='results'
 
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

def process_import_table(bq_client, input_table_id):
    if input_table_id is None:
        return None
    table_parts = input_table_id.split('.')
    table_name = table_parts[len(table_parts)-1]
    output_table_id = BQ_PROJECT+'.'+BQ_OUTPUT_DATASET+'.'+table_name

    # Script georeference matching in BigQuery
    query =f"""
-- Georeference matching script. Input table is expected to have been Location column 
-- names mapped to Darwin Core Location term names.
BEGIN
-- Make a table adding a field match_country to the input table, mapping the best existing
-- matching country field in the input, prioritizing by countrycode, then country.
-- First try filling from countrycode if it isn't NULL, country otherwise
CREATE TEMP TABLE countrify AS (
  SELECT 
    *,
    IF(countrycode IS NULL,country,countrycode) AS match_country
  FROM 
    `{input_table_id}`);
-- If that didn't work because at least one of the two country-ish fields didn't exist,
-- try filling from countrycode, assuming a missing country field was the cause.
EXCEPTION WHEN ERROR THEN
  BEGIN
  CREATE TEMP TABLE countrify AS (
    SELECT 
      *,
      countrycode AS match_country,
    FROM 
      `{input_table_id}`);
-- If that didn't work, because countrycode didn't exist in the input table,
-- try filling from country.
  EXCEPTION WHEN ERROR THEN
    BEGIN
    CREATE TEMP TABLE countrify AS (
    SELECT 
      *,
      country AS match_country,
    FROM 
      `{input_table_id}`);
    EXCEPTION WHEN ERROR THEN
-- If that didn't work, because country didn't exist in the input table, create the field
-- anyway and fill it with NULLs.
      CREATE TEMP TABLE countrify AS (
      SELECT 
        *,
        NULL AS match_country,
      FROM 
        `{input_table_id}`);
    END;
  END;
END;

CREATE TEMP TABLE interpreted AS (
SELECT 
  a.*,
  b.countrycode AS interpreted_countrycode, 
  GENERATE_UUID() AS id
FROM 
  countrify a 
LEFT JOIN
  `localityservice.vocabs.countrycode_lookup` b 
ON 
  UPPER(a.match_country)=b.u_country
);

-- Make the match strings
CREATE TEMP TABLE matcher AS (
SELECT
  id,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(functions.matchString(TO_JSON_STRING(t), "withcoords"))),NFKC)),r"[\s]+",'') AS matchwithcoords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(functions.matchString(TO_JSON_STRING(t), "verbatimcoords"))),NFKC)),r"[\s]+",'') AS matchverbatimcoords,
REGEXP_REPLACE(functions.saveNumbers(NORMALIZE_AND_CASEFOLD(functions.removeSymbols(functions.simplifyDiacritics(functions.matchString(TO_JSON_STRING(t), "sanscoords"))),NFKC)),r"[\s]+",'') AS matchsanscoords
FROM 
  interpreted AS t
);

-- CREATE table georefs from matchme_with_coords
CREATE TEMP TABLE georefs AS (
SELECT
  a.id,
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
  a.matchwithcoords=b.matchme_with_coords
);

-- APPEND verbatim coords matches to georefs
INSERT INTO georefs (
SELECT
  a.id,
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
  a.matchverbatimcoords=b.matchme AND
  a.id NOT IN (
SELECT 
  id
FROM georefs
)
);

-- APPEND sans coords matches to georefs
INSERT INTO georefs (
SELECT
  a.id,
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
  a.matchsanscoords=b.matchme_sans_coords AND
  a.id NOT IN (
SELECT 
  id
FROM georefs
)
);

-- Add georefs to original data as results
CREATE OR REPLACE TABLE `{output_table_id}`
AS
SELECT
  a.*,
  b.matchwithcoords,
  b.matchverbatimcoords,
  b.matchsanscoords,
  c.* EXCEPT (id)
FROM
  interpreted a
JOIN matcher b ON a.id=b.id
LEFT JOIN georefs c ON b.id=c.id;
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
    # logging.debug(f'table_id: {table_id} destination_uri: {destination_uri}')
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
    matchme_sans_coords as sans_coords_match_string,
    interpreted_countrycode as sans_coords_countrycode,
    interpreted_decimallatitude as sans_coords_decimallatitude,
    interpreted_decimallongitude as sans_coords_decimallongitude,
    unc_numeric as sans_coords_coordinateuncertaintyinmeters,
    v_georeferencedby as sans_coords_georeferencedby,
    v_georeferenceddate as sans_coords_georeferenceddate,
    v_georeferenceprotocol as sans_coords_georeferenceprotocol,
    v_georeferencesources as sans_coords_georeferencesources,
    v_georeferenceremarks as sans_coords_georeferenceremarks,
    georef_score as sans_coords_georef_score,
    centroid_dist as sans_coords_centroid_distanceinmeters,
    georef_count as sans_coords_georef_count
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
        return {
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
        'sans_coords_georef_count': None }
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
        return {
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
        'sans_coords_georef_count': None }
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
  matchme as verbatim_coords_match_string,
  interpreted_countrycode as verbatim_coords_countrycode,
  interpreted_decimallatitude as verbatim_coords_decimallatitude,
  interpreted_decimallongitude as verbatim_coords_decimallongitude,
  unc_numeric as verbatim_coords_coordinateuncertaintyinmeters,
  v_georeferencedby as verbatim_coords_georeferencedby,
  v_georeferenceddate as verbatim_coords_georeferenceddate,
  v_georeferenceprotocol as verbatim_coords_georeferenceprotocol,
  v_georeferencesources as verbatim_coords_georeferencesources,
  v_georeferenceremarks as verbatim_coords_georeferenceremarks,
  georef_score as verbatim_coords_georef_score,
  centroid_dist as verbatim_coords_centroid_distanceinmeters,
  georef_count as verbatim_coords_georef_count
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
        # Create a dict of an empty row so that every record can have a result
        # This has to match the structure of the rows query result.
        return {
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
        'sans_coords_georef_count': None }
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
  matchme_with_coords as with_coords_match_string,
  interpreted_countrycode as with_coords_countrycode,
  interpreted_decimallatitude as with_coords_decimallatitude,
  interpreted_decimallongitude as with_coords_decimallongitude,
  unc_numeric as with_coords_coordinateuncertaintyinmeters,
  v_georeferencedby as with_coords_georeferencedby,
  v_georeferenceddate as with_coords_georeferenceddate,
  v_georeferenceprotocol as with_coords_georeferenceprotocol,
  v_georeferencesources as with_coords_georeferencesources,
  v_georeferenceremarks as with_coords_georeferenceremarks,
  georef_score as with_coords_georef_score,
  centroid_dist as with_coords_centroid_distanceinmeters,
  georef_count as with_coords_georef_count
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
        # Create a dict of an empty row so that every record can have a result
        # This has to match the structure of the rows query result.
        return {
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
        'sans_coords_georef_count': None }
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
