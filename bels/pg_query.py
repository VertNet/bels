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

__author__ = "David Henderson"
__filename__ = "pg_query.py"
__version__ = __filename__ + ' ' + "2026-10-21T14:36-07:00"

"""
PostgreSQL versions of the gazetteer lookups used by /api/bestgeoref, for a gazetteer
snapshot loaded with postgres/load_snapshot.py. Each lookup returns the same fields,
with the same names, types and values, as the corresponding BigQuery *_reduced query
in bels_query.py.

Connection settings come from the environment variable BELS_PG_DSN (a libpq
connection string) or, if it is not set, the standard PostgreSQL environment variables
(PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD). BELS_PG_POOL_SIZE sets the maximum
number of connections per process (default 4).
"""

import os

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from bels_query import BELS_Client

PG_GAZ_SCHEMA = 'gazetteer'
PG_VOCABS_SCHEMA = 'vocabs'

# The columns of the BigQuery *_reduced queries, in the same order. In BigQuery,
# SAFE_CAST(round(unc_numeric,0) AS INT64) gives NULL outside the INT64 range; the CASE
# does the same. round() on numeric rounds halves away from zero in both.
REDUCED_QUERY = """
SELECT
  interpreted_countrycode AS bels_countrycode,
  {matchcolumn} AS bels_match_string,
  interpreted_decimallatitude AS bels_decimallatitude,
  interpreted_decimallongitude AS bels_decimallongitude,
  'epsg:4326'::text AS bels_geodeticdatum,
  CASE WHEN round(unc_numeric, 0) BETWEEN -9223372036854775808 AND 9223372036854775807
    THEN round(unc_numeric, 0)::bigint END AS bels_coordinateuncertaintyinmeters,
  v_georeferencedby AS bels_georeferencedby,
  v_georeferenceddate AS bels_georeferenceddate,
  v_georeferenceprotocol AS bels_georeferenceprotocol,
  v_georeferencesources AS bels_georeferencesources,
  v_georeferenceremarks AS bels_georeferenceremarks,
  georef_score AS bels_georeference_score,
  source AS bels_georeference_source,
  georef_count AS bels_best_of_n_georeferences,
  {matchtype}::text AS bels_match_type
FROM
  {table}
WHERE
  {matchcolumn} = %s
LIMIT 1
"""

def reduced_query(table, matchcolumn, matchtype):
    ''' Create the query to get the best georeference for a match string from one of
        the best georeference tables.
    parameters:
        table - name of the best georeference table in schema PG_GAZ_SCHEMA
        matchcolumn - name of the match string column of the table
        matchtype - value returned as bels_match_type
    returns:
        query - the query as a psycopg sql.Composed, with one parameter for the match
            string
    '''
    functionname = 'reduced_query()'

    return sql.SQL(REDUCED_QUERY).format(
        matchcolumn=sql.Identifier(matchcolumn),
        matchtype=sql.Literal(matchtype),
        table=sql.Identifier(PG_GAZ_SCHEMA, table))

WITH_COORDS_QUERY = reduced_query(
    'matchme_with_coords_best_georef', 'matchme_with_coords', 'match with coords')
VERBATIM_COORDS_QUERY = reduced_query(
    'matchme_verbatimcoords_best_georef', 'matchme', 'match using verbatim coords')
SANS_COORDS_QUERY = reduced_query(
    'matchme_sans_coords_best_georef', 'matchme_sans_coords', 'match sans coords')

class PG_BELS_Client(BELS_Client):
    ''' A BELS_Client that reads the gazetteer and the countrycode lookup from
        PostgreSQL instead of BigQuery.
    '''
    backend = 'PostgreSQL'

    def __init__(self, conninfo=None, pool_size=None):
        self.bq_client = None
        self.countrycode_dict = {}
        if conninfo is None:
            conninfo = os.getenv('BELS_PG_DSN', '')
        if pool_size is None:
            pool_size = int(os.getenv('BELS_PG_POOL_SIZE', '4'))
        self.conninfo = conninfo
        # The pool opens on first use rather than here, so that a server that imports
        # the app and then forks worker processes (e.g., gunicorn --preload) gives each
        # worker its own connections.
        self.pool = ConnectionPool(conninfo, min_size=1, max_size=pool_size,
            kwargs={'autocommit': True, 'row_factory': dict_row}, open=False,
            name='bels')

    def populate(self, table_id=None):
        ''' Load the countrycode lookup, as BELS_Client.populate() does from BigQuery.
            Uses its own connection, not the pool.
        parameters:
            table_id - schema-qualified name of the lookup table
                (default vocabs.countrycode_lookup)
        '''
        self.countrycode_dict = {}
        if table_id is None:
            table_id = PG_VOCABS_SCHEMA+'.'+'countrycode_lookup'
        query = sql.SQL('SELECT u_country, countrycode FROM {}').format(
            sql.Identifier(*table_id.split('.')))
        with psycopg.connect(self.conninfo) as conn:
            for row in conn.execute(query):
                self.countrycode_dict[row[0]]=row[1]

    def has_client(self):
        return self.pool is not None

    def lookup_with_coords(self, matchstr):
        return self.get_first_row(WITH_COORDS_QUERY, matchstr)

    def lookup_verbatim_coords(self, matchstr):
        return self.get_first_row(VERBATIM_COORDS_QUERY, matchstr)

    def lookup_sans_coords(self, matchstr):
        return self.get_first_row(SANS_COORDS_QUERY, matchstr)

    def get_first_row(self, query, matchstr):
        ''' Get the first row of a lookup query.
        parameters:
            query - one of the *_QUERY lookups
            matchstr - the match string to look up
        returns:
            row - the first row of the query result as a dict, or None if no row matched
        '''
        functionname = 'get_first_row()'

        # PostgreSQL text can not contain NUL, so no stored match string has one.
        if '\x00' in matchstr:
            return None
        self.pool.open()
        with self.pool.connection() as conn:
            return conn.execute(query, (matchstr,)).fetchone()
