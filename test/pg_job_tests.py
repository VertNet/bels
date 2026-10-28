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
__filename__ = "pg_job_tests.py"
__version__ = __filename__ + ' ' + "2026-10-21T16:06-07:00"

# This file tests the bulk path with PostgreSQL (bels/pg_job.py): uploads through
# /api/bels_csv, the job queue, the output file and the email, which is written to a
# file (BELS_EMAIL=log), never sent. The expected georeferences come from the snapshot
# files, as in pg_query_tests.py, which has the database settings. The tests that need
# the database are skipped if it can not be reached.
#
# Example:
#
# python pg_job_tests.py

import csv
import gzip
import io
import json
import logging
import os
import shutil
import tempfile
import unittest

from bulk_match import bulk_match_str, MATCHTYPES
from bels_query import bigquerify_header
from dwca_vocab_utils import darwinize_list
from parity_tests import rebuild_input
from pg_query_tests import check_database, get_test_conninfo, read_countrycodes
from pg_query_tests import read_snapshot, SNAPSHOTPATH
import pg_job

# Columns of the test upload: Darwin Core location terms, plus two others
UPLOAD_COLUMNS = ['countrycode', 'waterbody', 'island', 'stateprovince', 'county',
    'municipality', 'locality', 'verbatimlocality', 'minimumelevationinmeters',
    'maximumelevationinmeters', 'verbatimelevation', 'verbatimcoordinates',
    'verbatimlatitude', 'verbatimlongitude', 'decimallatitude', 'decimallongitude',
    'catalognumber', 'remarks']

# (table, match string column, bels_match_type), in order of preference
GEOREF_TABLES = [
    ('matchme_with_coords_best_georef', 'matchme_with_coords', 'match using coords'),
    ('matchme_verbatimcoords_best_georef', 'matchme', 'match using verbatim coords'),
    ('matchme_sans_coords_best_georef', 'matchme_sans_coords', 'match sans coords')
    ]

def expected_georef(row, matchtype):
    ''' The georefs columns of process_import_table() for a gazetteer row, as text. '''
    unc = row['unc_numeric']
    if unc is not None:
        unc = int(unc.quantize(1, rounding='ROUND_HALF_UP'))
    values = [row['interpreted_decimallatitude'], row['interpreted_decimallongitude'],
        None if row['interpreted_decimallatitude'] is None else 'epsg:4326', unc,
        row['v_georeferencedby'], row['v_georeferenceddate'],
        row['v_georeferenceprotocol'], row['v_georeferencesources'],
        row['v_georeferenceremarks'], row['georef_score'], row['source'],
        row['georef_count'], matchtype]
    return [csv_value(v) for v in values]

def csv_value(value):
    ''' A value of the output as csv.reader reads it back (NULL and "" both as ''). '''
    v = pg_job.output_value(value)
    return '' if v is None else v

class FakeSendGrid():
    ''' Records what would have been sent. '''
    def __init__(self):
        self.sent = []
        self.client = self
        self.mail = self
        self.send = self

    def post(self, request_body):
        self.sent.append(request_body)

class AllowlistMailerTestCase(unittest.TestCase):
    def message(self, to, bcc=None):
        p = {'to': [{'email': to}]}
        if bcc is not None:
            p['bcc'] = [{'email': bcc}]
        return {'subject': 's', 'personalizations': [p]}

    def test_allowlist(self):
        print('Running test_allowlist')
        sg = FakeSendGrid()
        mailer = pg_job.AllowlistMailer(sg, ['a@example.org'])
        mailer.client.mail.send.post(request_body=self.message('A@example.org', 'b@example.org'))
        self.assertEqual(sg.sent, [self.message('A@example.org')])
        mailer.client.mail.send.post(request_body=self.message('b@example.org'))
        self.assertEqual(len(sg.sent), 1)
        mailer = pg_job.AllowlistMailer(sg, ['*'])
        mailer.client.mail.send.post(request_body=self.message('b@example.org'))
        self.assertEqual(len(sg.sent), 2)

    def test_get_mailer(self):
        print('Running test_get_mailer')
        saved = {k: os.environ.get(k) for k in ['BELS_EMAIL', 'BELS_EMAIL_ALLOWLIST']}
        try:
            os.environ.pop('BELS_EMAIL', None)
            self.assertIsInstance(pg_job.get_mailer('x'), pg_job.LogMailer)
            os.environ['BELS_EMAIL'] = 'sendgrid'
            os.environ.pop('BELS_EMAIL_ALLOWLIST', None)
            with self.assertRaises(ValueError):
                pg_job.get_mailer('x')
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

class OutputFormatTestCase(unittest.TestCase):
    ''' Input and output CSV details checked against a live bulk job (2026-10-22). '''
    def test_output_value(self):
        print('Running test_output_value')
        self.assertIsNone(pg_job.output_value(None))
        self.assertEqual(pg_job.output_value(21.0), '21')
        self.assertEqual(pg_job.output_value(-74.0), '-74')
        self.assertEqual(pg_job.output_value(-32.322158), '-32.322158')
        self.assertEqual(pg_job.output_value(1083), '1083')
        self.assertEqual(pg_job.output_value(''), '')

    def test_csv_row(self):
        print('Running test_csv_row')
        self.assertEqual(pg_job.csv_row(['a', None, '', '  padded  ', 'a, b and "c"',
            'line one\nline two', 'Søndre Straße']),
            'a,,"",  padded  ,"a, b and ""c""","line one\nline two",Søndre Straße\n')

    def test_read_input_rows(self):
        print('Running test_read_input_rows')
        text = ('a,b,c,d\r\n'
            'x,"",,"q"\r\n'
            '"",y,"1\n2",""\r\n'
            '\r\n'
            'z,"say ""hi""","",\n')
        with tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False,
            encoding='utf-8', newline='') as f:
            f.write(text)
        try:
            rows = list(pg_job.read_input_rows(f.name, ['a', 'b', 'c', 'd']))
        finally:
            os.remove(f.name)
        self.assertEqual(rows, [['x', '', None, 'q'], ['', 'y', '1\n2', ''],
            ['z', 'say "hi"', '', None]])

    def test_quoted_empty_fields(self):
        print('Running test_quoted_empty_fields')
        self.assertEqual(pg_job.quoted_empty_fields('"",a,,""\r\n', 4),
            [True, False, False, True])
        self.assertEqual(pg_job.quoted_empty_fields('"a,""b""",""\n', 2), [False, True])
        # Not split the same way as csv.reader: no field taken as ""
        self.assertEqual(pg_job.quoted_empty_fields('a,""', 3), [False] * 3)

class PGJobTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conninfo = get_test_conninfo()
        check_database(cls.conninfo)
        cls.bulkdir = tempfile.mkdtemp(prefix='bels-bulk-test-')
        os.environ['BELS_BACKEND'] = 'postgres'
        os.environ['BELS_PG_DSN'] = cls.conninfo
        os.environ['BELS_BULK_DIR'] = cls.bulkdir
        os.environ['BELS_EMAIL'] = 'log'
        logging.basicConfig(level=logging.WARNING)
        cls.snapshot = {t: read_snapshot(t)[0] for t, c, m in GEOREF_TABLES}
        cls.countrycodes = read_countrycodes()
        # The app and the job read vocabulary files relative to the repository root.
        cls.cwd = os.getcwd()
        os.chdir('..')
        try:
            import api
        except Exception:
            os.chdir(cls.cwd)
            raise
        cls.api = api
        cls.app = api.app.test_client()
        cls.mailer = pg_job.LogMailer(os.path.join(cls.bulkdir, 'mail'))

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.cwd)
        cls.api.bels_client.pool.close()
        shutil.rmtree(cls.bulkdir)

    def submit(self, content, email='someone@example.org', filename='bulk test'):
        return self.app.post('/api/bels_csv', data={'email': email, 'filename': filename,
            'csv': (io.BytesIO(content.encode('utf-8')), 'upload.csv')},
            content_type='multipart/form-data')

    def run_job(self):
        ''' Run the queued job; return its name, final state and new mail messages. '''
        maildir = os.path.join(self.bulkdir, 'mail')
        before = set(os.listdir(maildir)) if os.path.isdir(maildir) else set()
        name = pg_job.run_next_job(self.bulkdir, self.api.bels_client, self.mailer)
        state = [s for s in ['done', 'failed']
            if os.path.exists(os.path.join(self.bulkdir, 'jobs', s, name))][0]
        after = set(os.listdir(maildir)) if os.path.isdir(maildir) else set()
        mails = []
        for m in sorted(after - before):
            with open(os.path.join(maildir, m), encoding='utf-8') as f:
                mails.append(json.load(f))
        return name, state, mails

    def upload_rows(self):
        ''' The test upload: every snapshot row as a user record, with
            interpreted_countrycode as countrycode, and a remarks value that needs
            quoting.
        '''
        rows = []
        for table, column, matchtype in GEOREF_TABLES:
            for r in self.snapshot[table].values():
                loc = rebuild_input(r)
                loc['catalognumber'] = f'{table[:12]}-{len(rows)}'
                loc['remarks'] = 'a, "b"\nc' if len(rows) % 7 == 0 else None
                rows.append([loc.get(c) for c in UPLOAD_COLUMNS])
        return rows

    def expected_output(self, header, rows):
        ''' The output rows, computed from the snapshot files. '''
        bqheader = bigquerify_header(darwinize_list(header, './bels/vocabularies/darwin_cloud.txt', case='l'))
        expected = []
        for values in rows:
            values = [None if v == '' else v for v in values]
            row = dict(zip(bqheader, values))
            country = row['countrycode']
            code = None if country is None else self.countrycodes.get(country.upper())
            row['bels_match_country'] = country
            row['bels_interpreted_countrycode'] = code
            strings = [bulk_match_str(row, t) for t in MATCHTYPES]
            georef = [''] * len(pg_job.GEOREF_COLUMNS)
            for (table, column, matchtype), s in zip(GEOREF_TABLES, strings):
                if s in self.snapshot[table]:
                    georef = expected_georef(self.snapshot[table][s], matchtype)
                    break
            expected.append([csv_value(v) for v in values + [country, code]
                + strings] + georef)
        return bqheader, expected

    def test_bulk_job(self):
        print('Running test_bulk_job')
        rows = self.upload_rows()
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator='\n')
        writer.writerow(UPLOAD_COLUMNS)
        writer.writerows([['' if v is None else v for v in r] for r in rows])
        response = self.submit(buf.getvalue())
        self.assertEqual(response.status_code, 200)
        self.assertIn('Submission received', response.get_data(as_text=True))
        name, state, mails = self.run_job()
        self.assertEqual(state, 'done')
        jobid = name.split('.')[0]
        outputpath = os.path.join(self.bulkdir, 'bels_output', f'bulk_test-{jobid}.csv.gz')
        with gzip.open(outputpath, 'rt', encoding='utf-8', newline='') as f:
            output = list(csv.reader(f))
        bqheader, expected = self.expected_output(UPLOAD_COLUMNS,
            [['' if v is None else v for v in r] for r in rows])
        self.assertEqual(output[0], bqheader + pg_job.ADDED_COLUMNS
            + pg_job.MATCH_COLUMNS + pg_job.GEOREF_COLUMNS)
        self.assertEqual(len(output) - 1, len(expected))
        mismatches = [i for i, (got, want) in enumerate(zip(output[1:], expected))
            if got != want]
        counts = {}
        for r in output[1:]:
            counts[r[-1] or 'no match'] = counts.get(r[-1] or 'no match', 0) + 1
        print(f'{len(expected)} rows: {counts}, {len(mismatches)} mismatches')
        self.assertEqual(mismatches, [], f'first: {output[1 + mismatches[0]] if mismatches else ""}')
        for matchtype in [m for t, c, m in GEOREF_TABLES]:
            self.assertGreater(counts.get(matchtype, 0), 0, matchtype)
        self.assertEqual(len(mails), 1)
        self.assertEqual(mails[0]['personalizations'][0]['to'][0]['email'],
            'someone@example.org')
        self.assertIn(os.path.abspath(outputpath), mails[0]['content'][0]['value'])

    def test_no_country_field(self):
        print('Running test_no_country_field')
        # api.py rejects such files, so this calls the job directly. The failure email
        # needs the From and Subject imports in job.py.
        message = {'data': {'upload_file_url': 'bels_input/none', 'email': 'someone@example.org',
            'output_filename': 'x', 'header': ['locality', 'decimallatitude']}}
        maildir = os.path.join(self.bulkdir, 'mail')
        before = set(os.listdir(maildir)) if os.path.isdir(maildir) else set()
        result = pg_job.process_csv_in_bulk_pg(message, self.api.bels_client, self.bulkdir,
            self.mailer)
        self.assertIsNone(result)
        new = sorted(set(os.listdir(maildir)) - before)
        self.assertEqual(len(new), 1)
        with open(os.path.join(maildir, new[0]), encoding='utf-8') as f:
            mail = json.load(f)
        self.assertEqual(mail['subject'], 'BELS - failure to process file')
        self.assertEqual(mail['from']['name'], 'BELS Georeference Matcher')

    def test_bad_rows_fail(self):
        print('Running test_bad_rows_fail')
        # As in BigQuery, a row with the wrong number of columns, or a column name that
        # the script also adds, fails the job; the user gets no email.
        for content in ['countrycode,locality\nUS,Foo\nUS,Foo,extra\n',
            'countrycode,locality,bels_match_type\nUS,Foo,x\n']:
            response = self.submit(content)
            self.assertEqual(response.status_code, 200)
            with self.assertLogs(level='ERROR'):
                name, state, mails = self.run_job()
            self.assertEqual(state, 'failed', content)
            self.assertEqual(mails, [], content)
        self.assertEqual([f for f in os.listdir(os.path.join(self.bulkdir, 'bels_output'))
            if f.endswith('.tmp')], [])

    def test_nul_in_value(self):
        print('Running test_nul_in_value')
        # A match string with NUL can not go into PostgreSQL text; the row gets no
        # georeference and its strings are rebuilt for the output.
        response = self.submit('countrycode,locality\nUS,Fo\x00o\nUS,Foo\n')
        self.assertEqual(response.status_code, 200)
        name, state, mails = self.run_job()
        self.assertEqual(state, 'done')
        outputpath = os.path.join(self.bulkdir, 'bels_output',
            f'bulk_test-{name.split(".")[0]}.csv.gz')
        with gzip.open(outputpath, 'rt', encoding='utf-8', newline='') as f:
            output = list(csv.reader(f))
        self.assertEqual(output[1][:4], ['US', 'Fo\x00o', 'US', 'US'])
        self.assertIn('\x00', output[1][4])
        self.assertEqual(output[2][4], 'usfoo')

    def test_invalid_email(self):
        print('Running test_invalid_email')
        response = self.submit('countrycode,locality\nUS,Foo\n', email='not-an-email')
        self.assertEqual(response.status_code, 400)

if __name__ == '__main__':
    print('=== pg_job_tests.py ===')
    unittest.main()
