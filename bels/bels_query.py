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
__version__ = "bels_query.py 2021-01-06T15:47-03:00"

from google.cloud import bigquery
from dwca_terms import locationkeytermlist
import json
from json_utils import CustomJsonEncoder

def query_location_by_id(base64locationhash):
    table_name = 'localityservice.gbif_20200409.locations_gbif_distinct'
    query ="""
        SELECT TO_BASE64(dwc_location_hash) as locationid, * EXCEPT (dwc_location_hash)
        FROM 
        {0}
        WHERE 
        TO_BASE64(dwc_location_hash)='{1}'
        """.format(table_name,base64locationhash)
    return query

def get_location_by_id(bq_client, base64locationhash):
    rows = run_bq_query(bq_client, query_location_by_id(base64locationhash), 1)
    for row in rows:
        return row_as_dict(row)

def query_best_sans_coords_georef(matchstr):
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
    rows = run_bq_query(bq_client, query_best_sans_coords_georef(matchstr), 1)
    for row in rows:
        return row_as_dict(row)

def query_best_with_verbatim_coords_georef(matchstr):
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
    rows = run_bq_query(bq_client, query_best_with_verbatim_coords_georef(matchstr), 1)
    for row in rows:
        return row_as_dict(row)

def query_best_with_coords_georef(matchstr):
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
    rows = run_bq_query(bq_client, query_best_with_coords_georef(matchstr), 1)
    for row in rows:
        return row_as_dict(row)

def run_bq_query(bq_client, query, max_results):
    query_job = bq_client.query(query)  # Make an API request.
    query_job.result()  # Wait for the query to complete.

    # Get the reference to the destination table for the query results.
    #
    # All queries write to a destination table. If a destination table is not
    # specified, the BigQuery populates it with a reference to a temporary
    # anonymous table after the query completes.
    destination = query_job.destination

    # Get the Table object from the destination table reference.
    destination = bq_client.get_table(destination)

    rows = bq_client.list_rows(destination, max_results=max_results)
    return rows

def row_as_list(row):
    row_list = list(row.items())
    return row_list

def row_as_dict(row):
    row_dict = {}
    for item in row_as_list(row):
        row_dict[item[0]]=item[1]
    return row_dict

def row_as_json(row):
    row_json=json.dumps(row_as_dict(row), cls=CustomJsonEncoder)
    return row_json
    
#def main():
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
