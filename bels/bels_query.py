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
__version__ = "bels_query.py 2021-01-07T13:50-03:00"

from google.cloud import bigquery
from .dwca_terms import locationkeytermlist
import json
from .json_utils import CustomJsonEncoder

def query_location_by_id(base64locationhash):
    ''' Create a query string to get a location record from the distinct Locations data
        store in BigQuery.
    parameters:
        base64locationhash - string representation of the base64 digest of the sha256
            location hash (what humans see in the BigQuery UI as dwc_location_hash).
    returns:
        query - the query string
    '''
    functionname = 'query_location_by_id()'

    table_name = 'localityservice.gbif_20200409.locations_distinct_with_scores'
    query ="""
        SELECT TO_BASE64(dwc_location_hash) as locationid, * EXCEPT (dwc_location_hash)
        FROM 
        {0}
        WHERE 
        TO_BASE64(dwc_location_hash)='{1}'
        """.format(table_name,base64locationhash)
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

def query_best_sans_coords_georef(matchstr):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the part of the 
        Location without coordinates from the Locations data store in BigQuery.
    parameters:
        matchstr - the matchme_sans_coords string to match.
    returns:
        query - the query string
    '''
    functionname = 'query_best_sans_coords_georef()'

    table_name = 'localityservice.gbif_20200409.matchme_sans_coords_best_georef'
    query ="""
        SELECT *
        FROM 
        {0}
        WHERE 
        matchme_sans_coords='{1}'
        """.format(table_name,matchstr)
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
    for row in rows:
        return row_as_dict(row)

def query_best_with_verbatim_coords_georef(matchstr):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the part of the 
        Location without coordinates, but with verbatim coordinates, from the Locations 
        data store in BigQuery.
    parameters:
        matchstr - the matchme string to match.
    returns:
        query - the query string
    '''
    functionname = 'query_best_with_verbatim_coords_georef()'

    table_name = 'localityservice.gbif_20200409.matchme_verbatimcoords_best_georef'
    query ="""
        SELECT *
        FROM 
        {0}
        WHERE 
        matchme='{1}'
        """.format(table_name,matchstr)
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

def query_best_with_coords_georef(matchstr):
    ''' Create a query string to get the best georeference for the location matching 
        string matchstr from among set of best georeferences using the whole 
        Location, with coordinates, from the Locations data store in BigQuery.
    parameters:
        matchstr - the matchme_with_coords string to match.
    returns:
        query - the query string
    '''
    functionname = 'query_best_with_coords_georef()'

    table_name = 'localityservice.gbif_20200409.matchme_with_coords_best_georef'
    query ="""
        SELECT *
        FROM 
        {0}
        WHERE 
        matchme_with_coords='{1}'
        """.format(table_name,matchstr)
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

    # Get a RowIterator with the results.
    # Note: a google.cloud.bigquery.table.RowIterator is not actually an Iterator, 
    # but rather an Iterable. It can be iterated over to get rows, but don't expect full
    # Iterator behavior.
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
    
def main():
    BQ = bigquery.Client()
    matchstr = 'auwac73kmsofbillabongroadhouse'
    result = get_best_sans_coords_georef(BQ, matchstr)
    print(result)

    matchstr = 'aqbechervaiseisland00-66.49559.49'
    result = get_best_with_coords_georef(BQ, matchstr)
    print(result)

    matchstr = 'usminnesotawadenat136nr33ws.1012-jul-71,'
    result = get_best_with_verbatim_coords_georef(BQ, matchstr)
    print(result)

if __name__ == '__main__':
    main()
