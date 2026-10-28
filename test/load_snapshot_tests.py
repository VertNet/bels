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
__filename__ = "load_snapshot_tests.py"
__version__ = __filename__ + ' ' + "2026-10-26T15:30-07:00"

# This file tests postgres/load_snapshot.py: loading the sample snapshot as one file
# per table and as numbered files (the layout of a BigQuery export), with several jobs,
# resuming an interrupted load, and stopping on a file with the wrong columns.
#
# The loader runs into the staging schema test_load_next, which is dropped afterwards,
# and is never run with --swap, so the published gazetteer schema is not changed. It
# does use vocabs.countrycode_lookup_next, so don't run these tests while a real load
# is running in the same database.
#
# The database comes from BELS_PG_DSN, the PG* environment variables, or the local
# database localityservice, as in pg_query_tests.py. The tests are skipped if the
# database can not be reached or psql (the PSQL environment variable, or psql on the
# PATH) is not found.
#
# Example:
#
# PSQL=/path/to/psql python load_snapshot_tests.py

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import psycopg
from psycopg import sql
import pyarrow.parquet as pq

from pg_query_tests import check_database
from pg_query_tests import get_test_conninfo

SNAPSHOTPATH = os.getenv('BELS_SNAPSHOT_DIR', '../bels_dev_snapshot')
LOADER = '../postgres/load_snapshot.py'
DDL = '../postgres/bels_runtime_ddl_postgresql.sql'
SCHEMA = 'test_load_next'
# table: lookup key
TABLES = {
    'matchme_with_coords_best_georef': 'matchme_with_coords',
    'matchme_verbatimcoords_best_georef': 'matchme',
    'matchme_sans_coords_best_georef': 'matchme_sans_coords'
    }
SLICES = 3

def make_export(snapshotdir, exportdir):
    ''' Write the sample snapshot as a BigQuery export would: each table as numbered
        GZIP Parquet files without extension, and the country CSV without extension.
    parameters:
        snapshotdir - the sample snapshot directory
        exportdir - the directory to write to
    returns:
        dict of table name: list of file paths
    '''
    files = {}
    for table in TABLES:
        data = pq.read_table(os.path.join(snapshotdir, table + '.parquet'))
        size = -(-data.num_rows // SLICES)
        files[table] = []
        for i in range(SLICES):
            path = os.path.join(exportdir, f'{table}{i:012d}')
            pq.write_table(data.slice(i * size, size), path, compression='gzip')
            files[table].append(path)
    shutil.copy(os.path.join(snapshotdir, 'countrycode_lookup.csv'),
                os.path.join(exportdir, 'countrycode_lookup'))
    return files

class LoadSnapshotTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conninfo = get_test_conninfo()
        check_database(cls.conninfo)
        if shutil.which(os.getenv('PSQL', 'psql')) is None:
            raise unittest.SkipTest('psql not found (set PSQL)')
        cls.exportdir = tempfile.mkdtemp(prefix='bels-export-')
        cls.exportfiles = make_export(SNAPSHOTPATH, cls.exportdir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.exportdir, ignore_errors=True)

    def tearDown(self):
        with psycopg.connect(self.conninfo) as conn:
            conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
                sql.Identifier(SCHEMA)))
            conn.execute('DROP TABLE IF EXISTS vocabs.countrycode_lookup_next')

    def load(self, snapshotdir, *options):
        ''' Run the loader into the test schema; return the exit status and the log. '''
        result = subprocess.run([sys.executable, LOADER, snapshotdir, '--ddl', DDL,
                                 '--schema', SCHEMA, '--dsn', self.conninfo, *options],
                                capture_output=True, text=True)
        return result.returncode, result.stderr

    def counts(self):
        ''' Get the row count of each staging table and the load_progress rows. '''
        with psycopg.connect(self.conninfo) as conn:
            counts = {table: conn.execute(sql.SQL('SELECT count(*) FROM {}').format(
                sql.Identifier(SCHEMA, table))).fetchone()[0] for table in TABLES}
            counts['countries'] = conn.execute(
                'SELECT count(*) FROM vocabs.countrycode_lookup_next').fetchone()[0]
            progress = conn.execute(sql.SQL(
                'SELECT step, count(*) FROM {} GROUP BY step').format(
                sql.Identifier(SCHEMA, 'load_progress'))).fetchall()
        return counts, dict(progress)

    def expected_counts(self):
        counts = {table: pq.ParquetFile(os.path.join(SNAPSHOTPATH, table + '.parquet'))
                  .metadata.num_rows for table in TABLES}
        # Some quoted country names contain line breaks
        with open(os.path.join(SNAPSHOTPATH, 'countrycode_lookup.csv'), newline='',
                  encoding='utf-8') as f:
            counts['countries'] = sum(1 for row in csv.reader(f)) - 1
        return counts

    def test_single_files(self):
        print('Running test_single_files')
        status, log = self.load(SNAPSHOTPATH)
        self.assertEqual(status, 0, log)
        self.assertIn('Verification passed', log)
        counts, progress = self.counts()
        self.assertEqual(counts, self.expected_counts())
        self.assertEqual(progress, {'load': 4, 'verify': 3})

    def test_export_files_parallel(self):
        print('Running test_export_files_parallel')
        status, log = self.load(self.exportdir, '--jobs', '2')
        self.assertEqual(status, 0, log)
        self.assertIn('Verification passed', log)
        counts, progress = self.counts()
        self.assertEqual(counts, self.expected_counts())
        self.assertEqual(progress, {'load': 3 * SLICES + 1, 'verify': 3 * SLICES})

    def test_resume(self):
        print('Running test_resume')
        status, log = self.load(self.exportdir, '--jobs', '2')
        self.assertEqual(status, 0, log)
        # Undo one file, as if the load had stopped before it was committed
        table = 'matchme_verbatimcoords_best_georef'
        path = self.exportfiles[table][1]
        keys = pq.read_table(path, columns=[TABLES[table]]).column(0).to_pylist()
        with psycopg.connect(self.conninfo) as conn:
            conn.execute(sql.SQL('DELETE FROM {} WHERE {} = ANY(%s)').format(
                sql.Identifier(SCHEMA, table), sql.Identifier(TABLES[table])), (keys,))
            conn.execute(sql.SQL('DELETE FROM {} WHERE file = %s').format(
                sql.Identifier(SCHEMA, 'load_progress')), (os.path.basename(path),))
        status, log = self.load(self.exportdir, '--resume', '--jobs', '2')
        self.assertEqual(status, 0, log)
        # Only that file is loaded and verified again; reloading any other file would
        # duplicate rows and fail the count check
        self.assertIn(f'Loaded {len(keys)} rows from 1 files into {SCHEMA}.{table}', log)
        self.assertIn(f'from 1 files: 0 problems', log)
        self.assertIn('Verification passed', log)
        counts, progress = self.counts()
        self.assertEqual(counts, self.expected_counts())
        self.assertEqual(progress, {'load': 3 * SLICES + 1, 'verify': 3 * SLICES})

    def test_resume_without_staging(self):
        print('Running test_resume_without_staging')
        status, log = self.load(SNAPSHOTPATH, '--resume')
        self.assertEqual(status, 0, log)
        self.assertIn(f'Nothing to resume in {SCHEMA}', log)
        counts, progress = self.counts()
        self.assertEqual(counts, self.expected_counts())

    def test_wrong_columns(self):
        print('Running test_wrong_columns')
        baddir = tempfile.mkdtemp(prefix='bels-export-bad-')
        try:
            for name in os.listdir(self.exportdir):
                shutil.copy(os.path.join(self.exportdir, name), baddir)
            table = 'matchme_sans_coords_best_georef'
            path = os.path.join(baddir, f'{table}{1:012d}')
            data = pq.read_table(path)
            pq.write_table(data.drop_columns(['georef_count']), path, compression='gzip')
            status, log = self.load(baddir, '--jobs', '2')
            self.assertNotEqual(status, 0)
            self.assertIn('Parquet columns do not match the table', log)
            self.assertIn("Missing: ['georef_count']", log)
        finally:
            shutil.rmtree(baddir, ignore_errors=True)

if __name__ == '__main__':
    print('=== load_snapshot_tests.py ===')
    unittest.main()
