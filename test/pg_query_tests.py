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
__filename__ = "pg_query_tests.py"
__version__ = __filename__ + ' ' + "2026-10-21T16:06-07:00"

# This file tests the PostgreSQL backend of /api/bestgeoref (bels/pg_query.py) against
# the snapshot files the database was loaded from with postgres/load_snapshot.py. The
# expected values are computed from the Parquet files, not from the database.
#
# The database connection comes from BELS_PG_DSN, or the PG* environment variables, or
# by default the database localityservice on the local server. If the database can not
# be reached, the tests are skipped. The snapshot directory can be changed with
# BELS_SNAPSHOT_DIR, as in parity_tests.py.
#
# The API test sends requests for the first 100 rows of each table. Set
# BELS_API_TEST_ROWS to change that, e.g. to 0 for all rows. Its expected responses
# assume the database holds only the snapshot rows: with the full gazetteer, requests
# built from verbatimcoords rows without verbatim coordinates can find a sans coords
# match that is not in the snapshot (67 requests with the default 100 rows per table),
# while the lookup test still passes.
#
# Example:
#
# python pg_query_tests.py

import contextlib
import csv
import io
import json
import logging
import os
import unittest
from decimal import Context, Decimal, ROUND_HALF_UP

import psycopg
import pyarrow.parquet as pq

from bels_query import has_decimal_coords
from bels_query import has_verbatim_coords
from dwca_terms import locationmatchwithcoordstermlist
from dwca_terms import locationmatchverbatimcoordstermlist
from dwca_terms import locationmatchsanscoordstermlist
from id_utils import location_match_str
from id_utils import super_simplify
from parity_tests import rebuild_input
from pg_query import PG_BELS_Client

# table: (match string column, bels_match_type, PG_BELS_Client lookup, match terms)
LOOKUPS = {
    'matchme_with_coords_best_georef':
        ('matchme_with_coords', 'match with coords', 'lookup_with_coords',
         locationmatchwithcoordstermlist),
    'matchme_verbatimcoords_best_georef':
        ('matchme', 'match using verbatim coords', 'lookup_verbatim_coords',
         locationmatchverbatimcoordstermlist),
    'matchme_sans_coords_best_georef':
        ('matchme_sans_coords', 'match sans coords', 'lookup_sans_coords',
         locationmatchsanscoordstermlist)
    }

# Absolute, because PGApiTestCase changes to the repository root
SNAPSHOTPATH = os.path.abspath(os.getenv('BELS_SNAPSHOT_DIR', '../bels_dev_snapshot/'))
API_TEST_ROWS = int(os.getenv('BELS_API_TEST_ROWS', '100'))

def get_test_conninfo():
    ''' Get the connection string for the test database.
    returns:
        conninfo - BELS_PG_DSN if set, '' (PG* variables) if PGDATABASE is set,
            otherwise the local database localityservice
    '''
    conninfo = os.getenv('BELS_PG_DSN')
    if conninfo is None:
        conninfo = '' if os.getenv('PGDATABASE') else 'dbname=localityservice'
    return conninfo

def check_database(conninfo):
    ''' Skip the tests if the database can not be reached. '''
    try:
        with psycopg.connect(conninfo, connect_timeout=5):
            pass
    except psycopg.OperationalError as e:
        raise unittest.SkipTest(f'PostgreSQL database not available: {e}')

def read_snapshot(table):
    ''' Get the rows of one snapshot table, keyed by match string.
    parameters:
        table - name of the best georeference table
    returns:
        rows - dict of match string: row
        duplicates - list of match strings that occur more than once
    '''
    column = LOOKUPS[table][0]
    rows = {}
    duplicates = []
    parquetfile = pq.ParquetFile(os.path.join(SNAPSHOTPATH, table + '.parquet'))
    for batch in parquetfile.iter_batches(batch_size=10000):
        for row in batch.to_pylist():
            if row[column] in rows:
                duplicates.append(row[column])
            rows[row[column]] = row
    return rows, duplicates

def read_countrycodes():
    ''' Get the countrycode lookup from the snapshot CSV, with empty values as None,
        as COPY loads them.
    '''
    countrycodes = {}
    path = os.path.join(SNAPSHOTPATH, 'countrycode_lookup.csv')
    with open(path, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            countrycodes[row['u_country'] or None] = row['countrycode'] or None
    return countrycodes

def expected_result(table, row):
    ''' Get the result the BigQuery *_reduced query returns for a snapshot row.
    parameters:
        table - name of the best georeference table
        row - the snapshot row
    returns:
        result - dict of the result fields, in query order
    '''
    column, matchtype = LOOKUPS[table][:2]
    # BigQuery SAFE_CAST(round(unc_numeric,0) AS INT64): halves away from zero, NULL
    # outside the INT64 range
    unc = row['unc_numeric']
    if unc is not None:
        unc = int(unc.quantize(Decimal(1), context=Context(prec=50, rounding=ROUND_HALF_UP)))
        if unc < -2**63 or unc >= 2**63:
            unc = None
    return {
        'bels_countrycode': row['interpreted_countrycode'],
        'bels_match_string': row[column],
        'bels_decimallatitude': row['interpreted_decimallatitude'],
        'bels_decimallongitude': row['interpreted_decimallongitude'],
        'bels_geodeticdatum': 'epsg:4326',
        'bels_coordinateuncertaintyinmeters': unc,
        'bels_georeferencedby': row['v_georeferencedby'],
        'bels_georeferenceddate': row['v_georeferenceddate'],
        'bels_georeferenceprotocol': row['v_georeferenceprotocol'],
        'bels_georeferencesources': row['v_georeferencesources'],
        'bels_georeferenceremarks': row['v_georeferenceremarks'],
        'bels_georeference_score': row['georef_score'],
        'bels_georeference_source': row['source'],
        'bels_best_of_n_georeferences': row['georef_count'],
        'bels_match_type': matchtype
        }

def typed(d):
    ''' Get a dict as a list of (key, type name, value), so that comparisons also check
        key order and types (in Python, 25 == 25.0).
    '''
    if d is None:
        return None
    return [(key, type(value).__name__, value) for key, value in d.items()]

class PGQueryTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conninfo = get_test_conninfo()
        check_database(cls.conninfo)
        cls.client = PG_BELS_Client(conninfo=cls.conninfo)

    @classmethod
    def tearDownClass(cls):
        cls.client.pool.close()

    def test_countrycode_lookup(self):
        print('Running test_countrycode_lookup')
        self.client.populate()
        self.assertEqual(self.client.countrycode_dict, read_countrycodes())

    def test_lookups(self):
        print('Running test_lookups')
        for table, (column, matchtype, lookup, terms) in LOOKUPS.items():
            rows, duplicates = read_snapshot(table)
            self.assertEqual(duplicates, [], f'{table}: match strings not unique')
            mismatches = []
            for matchstr, row in rows.items():
                result = getattr(self.client, lookup)(matchstr)
                if typed(result) != typed(expected_result(table, row)):
                    mismatches.append(f'{table} {matchstr!r}\n  got: {typed(result)}')
            print(f'{table}: {len(rows)} lookups, {len(mismatches)} mismatches')
            self.assertEqual(len(mismatches), 0, '\n'.join(mismatches[:5]))

    def test_no_match(self):
        print('Running test_no_match')
        for column, matchtype, lookup, terms in LOOKUPS.values():
            self.assertIsNone(getattr(self.client, lookup)('nosuchlocationanywhere'))
            # PostgreSQL text can not hold NUL; the lookup returns no match, not an error
            self.assertIsNone(getattr(self.client, lookup)('no\x00such'))

class PGApiTestCase(unittest.TestCase):
    ''' Requests to /api/bestgeoref through the Flask app with BELS_BACKEND=postgres.
        For each snapshot row, the request has the row's verbatim values of the match
        terms of its table, with interpreted_countrycode as countrycode, and no
        georeference, so the API looks it up instead of returning the original. The
        expected response follows the order of lookups in resources.BestGeoref.
    '''
    @classmethod
    def setUpClass(cls):
        cls.conninfo = get_test_conninfo()
        check_database(cls.conninfo)
        # The app reads its settings from the environment and its vocabulary files from
        # paths relative to the repository root.
        os.environ['BELS_BACKEND'] = 'postgres'
        os.environ['BELS_PG_DSN'] = cls.conninfo
        logging.basicConfig(level=logging.WARNING)
        cls.snapshot = {table: read_snapshot(table)[0] for table in LOOKUPS}
        cls.cwd = os.getcwd()
        os.chdir('..')
        try:
            import api
        except Exception:
            # tearDownClass is not called if setUpClass fails
            os.chdir(cls.cwd)
            raise
        cls.api = api
        cls.app = api.app.test_client()

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.cwd)
        cls.api.bels_client.pool.close()

    def expected_lookup(self, request_loc):
        ''' Get the expected result of a request, from the snapshot files. '''
        lowerloc = dict(request_loc)
        lowerloc['countrycode'] = self.api.bels_client.get_best_countrycode(lowerloc)
        steps = []
        if has_decimal_coords(lowerloc) == True:
            steps.append('matchme_with_coords_best_georef')
        if has_verbatim_coords(lowerloc):
            steps.append('matchme_verbatimcoords_best_georef')
        steps.append('matchme_sans_coords_best_georef')
        for table in steps:
            matchstr = super_simplify(location_match_str(LOOKUPS[table][3], lowerloc))
            row = self.snapshot[table].get(matchstr)
            if row is not None:
                return expected_result(table, row)
        return None

    def test_bestgeoref(self):
        print('Running test_bestgeoref')
        outcomes = {}
        mismatches = []
        skipped = 0
        for table, (column, matchtype, lookup, terms) in LOOKUPS.items():
            rows = list(self.snapshot[table].values())
            if API_TEST_ROWS > 0:
                rows = rows[:API_TEST_ROWS]
            for row in rows:
                loc = rebuild_input(row)
                if loc['countrycode'] is None:
                    skipped += 1
                    continue
                request_loc = {t: loc[t] for t in terms if loc.get(t) is not None}
                expected = self.expected_lookup(request_loc)
                with contextlib.redirect_stdout(io.StringIO()):
                    # Serialized here because the test client's json= sorts the keys,
                    # and the response repeats the request fields in their order.
                    response = self.app.post('/api/bestgeoref',
                        data=json.dumps({'give_me': 'BEST_GEOREF',
                            'for_location': request_loc}),
                        content_type='application/json')
                message = response.get_json()['Message']
                if expected is None:
                    outcome = 'failure'
                    expected_message = [('status', 'failure'), ('result', request_loc)]
                else:
                    outcome = expected['bels_match_type']
                    expected_message = [('status', 'success'), ('elapsed_time', None),
                        ('result', dict(request_loc, **expected))]
                outcomes[(table, outcome)] = outcomes.get((table, outcome), 0) + 1
                got = [(k, None if k == 'elapsed_time' else v) for k, v in message.items()]
                got = [(k, typed(v) if k == 'result' else v) for k, v in got]
                expected_message = [(k, typed(v) if k == 'result' else v)
                    for k, v in expected_message]
                if response.status_code != 200 or got != expected_message:
                    mismatches.append(f'{table} {request_loc}\n  got: {message}')
        for (table, outcome), count in sorted(outcomes.items()):
            print(f'{table}: {outcome}: {count}')
        print(f'Skipped {skipped} rows without interpreted_countrycode, '
              f'{len(mismatches)} mismatches')
        self.assertEqual(len(mismatches), 0, '\n'.join(mismatches[:5]))
        # Each lookup must have been exercised, so the test can not pass vacuously.
        for table, (column, matchtype, lookup, terms) in LOOKUPS.items():
            self.assertGreater(outcomes.get((table, matchtype), 0), 0, matchtype)

    def test_bels_csv_not_available(self):
        print('Running test_bels_csv_not_available')
        # Without BELS_BULK_DIR there is nowhere to queue jobs (see pg_job_tests.py).
        bulkdir = os.environ.pop('BELS_BULK_DIR', None)
        try:
            response = self.app.post('/api/bels_csv', data={'email': 'someone@example.org'})
        finally:
            if bulkdir is not None:
                os.environ['BELS_BULK_DIR'] = bulkdir
        self.assertEqual(response.status_code, 503)

if __name__ == '__main__':
    print('=== pg_query_tests.py ===')
    unittest.main()
