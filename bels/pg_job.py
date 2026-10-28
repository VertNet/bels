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
__filename__ = "pg_job.py"
__version__ = __filename__ + ' ' + "2026-10-21T16:06-07:00"

"""
The bulk path (/api/bels_csv) with PostgreSQL, in place of Cloud Storage, Pub/Sub, the
Cloud Function in job.py and the BigQuery script in process_import_table().

api.py saves each upload with submit_job() in a directory, BELS_BULK_DIR:
    bels_input/<uuid>              the uploaded file, as uploaded
    jobs/queue/<uuid>.json         the job, the same message api.py publishes to Pub/Sub
    jobs/running, done, failed     where the worker moves a job as it runs
    bels_output/<name>-<uuid>.csv.gz   the results
    mail/                          email messages, if BELS_EMAIL is 'log'

A worker process runs the jobs, from the repository root:
    BELS_BULK_DIR=... python bels/pg_job.py [--once]

process_csv_in_bulk_pg() follows job.process_csv_in_bulk() step by step and builds the
same output table that process_import_table() builds in BigQuery: the input columns, as
loaded (all STRING), then bels_match_country, bels_interpreted_countrycode, the three
match strings (bulk_match.py) and the georeference of the first match with coords,
verbatim coords, or sans coords. Rows are written in input order.

Settings, from environment variables:
    BELS_BULK_DIR - the directory above (required)
    BELS_BULK_URL - base URL under which bels_output/ is served, for the links in the
        email (default: the file path)
    BELS_EMAIL - 'log' (default) writes each message to mail/ instead of sending it;
        'sendgrid' sends with SENDGRID_API_KEY, to the recipients in
        BELS_EMAIL_ALLOWLIST only (comma-separated addresses, or * for anyone)
    BELS_PG_DSN or PG* - the database connection, as in pg_query.py
"""

import argparse
import csv
import gzip
import io
import json
import logging
import os
import re
import sys
import time
import types
import uuid

import psycopg

from bulk_match import bulk_match_str, MATCHTYPES
from bels_query import bigquerify_header, country_fields
from dwca_vocab_utils import darwinize_list
from pg_query import PG_BELS_Client, PG_GAZ_SCHEMA
import job

INPUT_LOCATION = 'bels_input'
OUTPUT_LOCATION = 'bels_output'
JOB_STATES = ['queue', 'running', 'done', 'failed']

# Columns added by process_import_table(), in output order. A file that already has one
# of these columns fails in BigQuery (duplicate column names), and fails here too.
ADDED_COLUMNS = ['bels_match_country', 'bels_interpreted_countrycode']
MATCH_COLUMNS = ['bels_matchwithcoords', 'bels_matchverbatimcoords',
    'bels_matchsanscoords']
GEOREF_COLUMNS = ['bels_decimallatitude', 'bels_decimallongitude', 'bels_geodeticdatum',
    'bels_coordinateuncertaintyinmeters', 'bels_georeferencedby', 'bels_georeferenceddate',
    'bels_georeferenceprotocol', 'bels_georeferencesources', 'bels_georeferenceremarks',
    'bels_georeference_score', 'bels_georeference_source', 'bels_best_of_n_georeferences',
    'bels_match_type']

# The georefs table of process_import_table(), one lookup per table in order of
# preference. The first match wins, as with the INSERTs ... NOT IN (SELECT bels_id ...).
GEOREF_SELECT = """
  SELECT
    {priority} AS priority,
    interpreted_decimallatitude AS bels_decimallatitude,
    interpreted_decimallongitude AS bels_decimallongitude,
    CASE WHEN interpreted_decimallatitude IS NULL THEN NULL ELSE 'epsg:4326' END
      AS bels_geodeticdatum,
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
    '{matchtype}'::text AS bels_match_type
  FROM {schema}.{table} b
  WHERE b.{key} = m.{matchcolumn}"""
GEOREF_LOOKUPS = [
    ('matchme_with_coords_best_georef', 'matchme_with_coords', 'withcoords',
     'match using coords'),
    ('matchme_verbatimcoords_best_georef', 'matchme', 'verbatimcoords',
     'match using verbatim coords'),
    ('matchme_sans_coords_best_georef', 'matchme_sans_coords', 'sanscoords',
     'match sans coords')
    ]
GEOREF_QUERY = 'SELECT m.rowid, m.withcoords, m.verbatimcoords, m.sanscoords, g.*' + \
    ' FROM matcher m LEFT JOIN LATERAL (' + \
    '\n  UNION ALL'.join(GEOREF_SELECT.format(priority=i, schema=PG_GAZ_SCHEMA,
        table=table, key=key, matchcolumn=matchtype, matchtype=label)
        for i, (table, key, matchtype, label) in enumerate(GEOREF_LOOKUPS)) + \
    '\n  ORDER BY priority LIMIT 1) g ON true\nORDER BY m.rowid'

class JobError(Exception):
    ''' A job that fails where the BigQuery job fails without telling the user (the load,
        the script or the export raises). Logged, and the job moves to jobs/failed.
    '''

def bulk_dir():
    return os.getenv('BELS_BULK_DIR')

def submit_job(bulkdir, csv_content, email, filename, header):
    ''' Save an upload and queue it, in place of the Cloud Storage upload and the Pub/Sub
        message in api.bels_csv().
    parameters:
        bulkdir - the BELS_BULK_DIR directory
        csv_content - the uploaded file, bytes
        email - address for the notification
        filename - the cleaned output file name
        header - the cleaned field names from the first line of the file
    returns:
        message_json - the job message, as published to Pub/Sub
    '''
    functionname = 'submit_job()'

    for d in [INPUT_LOCATION] + [os.path.join('jobs', s) for s in JOB_STATES]:
        os.makedirs(os.path.join(bulkdir, d), exist_ok=True)
    jobid = str(uuid.uuid4())
    blob_location = f'{INPUT_LOCATION}/{jobid}'
    with open(os.path.join(bulkdir, blob_location), 'wb') as f:
        f.write(csv_content)
    message_json = json.dumps({
        'data': {
            'upload_file_url': blob_location,
            'email': email,
            'output_filename': filename,
            'header' : header,
        }
    })
    # Write, then rename, so the worker never sees a partial job file.
    queued = os.path.join(bulkdir, 'jobs', 'queue', jobid + '.json')
    with open(queued + '.tmp', 'w', encoding='utf-8') as f:
        f.write(message_json)
    os.rename(queued + '.tmp', queued)
    return message_json

class LogMailer():
    ''' Stands in for the SendGrid client in job.send_email() and
        job.send_failure_email(), which call sg_api.client.mail.send.post(request_body).
        Writes each message to a JSON file in maildir instead of sending it.
    '''
    def __init__(self, maildir):
        self.maildir = maildir
        self.client = types.SimpleNamespace(
            mail=types.SimpleNamespace(send=types.SimpleNamespace(post=self.post)))

    def post(self, request_body):
        os.makedirs(self.maildir, exist_ok=True)
        name = time.strftime('%Y%m%dT%H%M%S') + '-' + str(uuid.uuid4())[:8] + '.json'
        with open(os.path.join(self.maildir, name), 'w', encoding='utf-8') as f:
            json.dump(request_body, f, indent=2, ensure_ascii=False)
        logging.info(f'Email not sent (BELS_EMAIL=log), saved as {name}')

class AllowlistMailer():
    ''' Wraps the SendGrid client so that messages go only to allowed recipients. Others
        are removed from to, cc and bcc; a message with no allowed to is not sent.
    '''
    def __init__(self, sg_api, allowlist):
        self.sg_api = sg_api
        self.allowlist = allowlist
        self.client = types.SimpleNamespace(
            mail=types.SimpleNamespace(send=types.SimpleNamespace(post=self.post)))

    def allowed(self, address):
        return '*' in self.allowlist or address.lower() in self.allowlist

    def post(self, request_body):
        personalizations = []
        for p in request_body.get('personalizations', []):
            p = dict(p)
            for field in ['to', 'cc', 'bcc']:
                if field in p:
                    removed = [a['email'] for a in p[field] if not self.allowed(a['email'])]
                    if len(removed) > 0:
                        logging.warning(f'Email not sent to {removed} (not in BELS_EMAIL_ALLOWLIST)')
                    p[field] = [a for a in p[field] if self.allowed(a['email'])]
                    if len(p[field]) == 0:
                        del p[field]
            if 'to' in p:
                personalizations.append(p)
        if len(personalizations) == 0:
            return
        self.sg_api.client.mail.send.post(
            request_body=dict(request_body, personalizations=personalizations))

def get_mailer(bulkdir):
    ''' Get the mail client chosen by BELS_EMAIL. '''
    mode = os.getenv('BELS_EMAIL', 'log').lower()
    if mode == 'log':
        return LogMailer(os.path.join(bulkdir, 'mail'))
    if mode == 'sendgrid':
        allowlist = [a.strip().lower() for a in os.getenv('BELS_EMAIL_ALLOWLIST', '').split(',')
            if a.strip() != '']
        if len(allowlist) == 0:
            raise ValueError('BELS_EMAIL=sendgrid needs BELS_EMAIL_ALLOWLIST')
        return AllowlistMailer(job.sg_api, allowlist)
    raise ValueError(f"BELS_EMAIL must be 'log' or 'sendgrid', not '{mode}'")

class RecordingLines:
    ''' Line iterator for csv.reader that keeps the raw lines of the current record. '''
    def __init__(self, f):
        self.f = f
        self.lines = []

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.f)
        self.lines.append(line)
        return line

    def take(self):
        ''' Get the raw text of the record just read and start the next one. '''
        raw = ''.join(self.lines)
        self.lines = []
        return raw

# One field of a raw CSV record: quoted, or anything up to the next comma.
CSV_FIELD = re.compile(r'"(?:[^"]|"")*"|[^,]*')

def quoted_empty_fields(raw, nfields):
    ''' Find the fields of a raw CSV record written as "" (a quoted empty string).
    parameters:
        raw - the raw text of one record
        nfields - the number of fields csv.reader found in it
    returns:
        a list of nfields booleans, all False if the record can not be split the same way
    '''
    raw = raw.rstrip('\r\n')
    tokens = []
    pos = 0
    while True:
        m = CSV_FIELD.match(raw, pos)
        tokens.append(m.group())
        pos = m.end()
        if pos >= len(raw) or raw[pos] != ',':
            break
        pos += 1
    if len(tokens) != nfields:
        return [False] * nfields
    return [t == '""' for t in tokens]

def read_input_rows(path, header):
    ''' Read the data rows of an uploaded file as BigQuery loads it in import_table():
        UTF-8, the first row skipped, quoted newlines allowed, every column STRING, empty
        values NULL except a quoted "", which is an empty string. Like the BigQuery
        load, fails if a row has a different number of columns than the header.
    parameters:
        path - the uploaded file
        header - the column names
    returns:
        an iterator of lists of str or None
    '''
    functionname = 'read_input_rows()'

    with open(path, newline='', encoding='utf-8') as f:
        lines = RecordingLines(f)
        reader = csv.reader(lines)
        try:
            next(reader, None)
            lines.take()
            for row in reader:
                raw = lines.take()
                if len(row) == 0:
                    continue
                if len(row) != len(header):
                    raise JobError(f'Row {reader.line_num} has {len(row)} columns, '
                        f'the header has {len(header)}.')
                if '' in row and '""' in raw:
                    quoted = quoted_empty_fields(raw, len(row))
                    yield [None if v == '' and not q else v for v, q in zip(row, quoted)]
                else:
                    yield [None if v == '' else v for v in row]
        except (UnicodeDecodeError, csv.Error) as e:
            raise JobError(f'Unable to load {path}: {e}')

def output_value(value):
    ''' A value as written in the BigQuery CSV export, before quoting (NULL as None).
        FLOAT64 is written like repr() but without a trailing ".0" (21.0 as 21).
    '''
    if value is None:
        return None
    if isinstance(value, float):
        s = repr(value)
        return s[:-2] if s.endswith('.0') else s
    return str(value)

def csv_row(values):
    ''' A row of the BigQuery CSV export: NULL as an empty field, an empty string as "",
        other values quoted only if they contain a comma, quote or line break.
    parameters:
        values - list of values as returned by output_value()
    returns:
        the line, ending in '\\n'
    '''
    fields = []
    for v in values:
        if v is None:
            fields.append('')
        elif v == '' or any(c in v for c in ',"\r\n'):
            fields.append('"' + v.replace('"', '""') + '"')
        else:
            fields.append(v)
    return ','.join(fields) + '\n'

def country_columns(row, countryfieldlist, client):
    ''' Get bels_match_country (the first non-NULL country field, as in
        matchcountryclause) and bels_interpreted_countrycode (UPPER(bels_match_country)
        looked up in countrycode_lookup) for an input row.
    returns:
        [bels_match_country, bels_interpreted_countrycode]
    '''
    matchcountry = None
    for field in countryfieldlist:
        if row[field] is not None:
            matchcountry = row[field]
            break
    countrycode = None
    if matchcountry is not None:
        countrycode = client.countrycode_dict.get(matchcountry.upper())
    return [matchcountry, countrycode]

def interpret_row(header, values, countryfieldlist, client):
    ''' Get the columns process_import_table() adds to an input row.
    returns:
        added - [bels_match_country, bels_interpreted_countrycode]
        strings - the match strings, in the order of MATCHTYPES
    '''
    row = dict(zip(header, values))
    added = country_columns(row, countryfieldlist, client)
    row['bels_match_country'] = added[0]
    row['bels_interpreted_countrycode'] = added[1]
    strings = [bulk_match_str(row, t) for t in MATCHTYPES]
    return added, strings

def match_rows(client, inputpath, outputpath, header, countryfieldlist):
    ''' Match the rows of an input file with the gazetteer and write the output table.
    parameters:
        client - a PG_BELS_Client with the countrycode lookup populated
        inputpath - the uploaded file
        outputpath - the gzipped CSV file to write
        header - the column names of the input (bigquerified)
        countryfieldlist - the country fields of the header, from country_fields()
    returns:
        nrows - the number of rows
        nmatched - the number of rows with a georeference
    '''
    functionname = 'match_rows()'

    nrows = 0
    nmatched = 0
    with psycopg.connect(client.conninfo) as conn:
        conn.execute("""CREATE TEMP TABLE matcher (rowid bigint, withcoords text,
            verbatimcoords text, sanscoords text) ON COMMIT DROP""")
        # Pass 1: the match strings of every row, as the countrify, interpreted and
        # matcher tables of the BigQuery script.
        with conn.cursor().copy('COPY matcher FROM STDIN') as copy:
            for values in read_input_rows(inputpath, header):
                added, strings = interpret_row(header, values, countryfieldlist, client)
                # PostgreSQL text can not hold NUL, and no gazetteer string has one.
                copy.write_row([nrows] + [None if '\x00' in s else s for s in strings])
                nrows += 1
        conn.execute('ANALYZE matcher')
        # Pass 2: read the input again alongside the match strings and georeferences,
        # in input order, so that no more than one row is in memory.
        with conn.cursor(name='georefs') as cur, \
            gzip.open(outputpath, 'wt', encoding='utf-8', newline='') as out:
            out.write(csv_row(header + ADDED_COLUMNS + MATCH_COLUMNS + GEOREF_COLUMNS))
            cur.execute(GEOREF_QUERY)
            matches = iter(cur)
            for rowid, values in enumerate(read_input_rows(inputpath, header)):
                matchrow = next(matches)
                if matchrow[0] != rowid:
                    raise JobError(f'Row {rowid} out of step with georeferences.')
                strings = list(matchrow[1:4])
                if None in strings:
                    # A string with NUL was not stored; build it again.
                    added, strings = interpret_row(header, values, countryfieldlist,
                        client)
                else:
                    added = country_columns(dict(zip(header, values)), countryfieldlist,
                        client)
                if matchrow[4] is not None:
                    nmatched += 1
                out.write(csv_row([output_value(v) for v in
                    values + added + strings + list(matchrow[5:])]))
    return nrows, nmatched

def process_csv_in_bulk_pg(json_config_in, client, bulkdir, mailer, outputurl=None):
    ''' Georeference all the rows of an uploaded file, as job.process_csv_in_bulk() does
        with BigQuery.
    parameters:
        json_config_in - the job message from submit_job(), as a dict
        client - a PG_BELS_Client with the countrycode lookup populated
        bulkdir - the BELS_BULK_DIR directory
        mailer - a LogMailer, an AllowlistMailer or a SendGrid client
        outputurl - base URL for bels_output (default: the local directory)
    returns:
        the list of output URLs, or None if no output was made
    '''
    functionname = 'process_csv_in_bulk_pg()'

    starttime = time.perf_counter()
    json_config = json_config_in['data']
    upload_file_url = json_config['upload_file_url']
    output_filename = json_config['output_filename']
    email = json_config['email']
    header = json_config['header']

    # Output file name [filename]-[UUID of input file location].csv.gz, as in job.py
    upload_file_parts = upload_file_url.split('/')
    file_suffix = upload_file_parts[len(upload_file_parts)-1].split('.')[0]
    output_filename = re.sub(r'[ ~`!@#$%^&*()_+={\[}\]|\\:;"<,>?\'/]', '_', output_filename)
    output_filename = '%s-%s.csv.gz' % (output_filename, file_suffix)

    dwccloudfile = './bels/vocabularies/darwin_cloud.txt'
    darwinized_header = darwinize_list(header, dwccloudfile, case='l')
    bigqueryized_header = bigquerify_header(darwinized_header)

    countryfieldlist = country_fields(bigqueryized_header)
    if countryfieldlist is None:
        s = f'The file {upload_file_url} has no interpretable country field '
        s += f'(interpreted_countrycode, countrycode, v_countrycode, country) '
        s += f'in the header after processing:\n{bigqueryized_header}.'
        logging.error(s)
        job.send_failure_email(email, s, mailer)
        return None

    # BigQuery column names are case-insensitive and must be unique, also with the
    # columns the script adds.
    names = [c.lower() for c in bigqueryized_header]
    reserved = ADDED_COLUMNS + MATCH_COLUMNS + GEOREF_COLUMNS + ['bels_id']
    for c in names:
        if names.count(c) > 1 or c in reserved:
            raise JobError(f'Duplicate column name {c} in {bigqueryized_header}.')

    inputpath = os.path.join(bulkdir, upload_file_url)
    outputpath = os.path.join(bulkdir, OUTPUT_LOCATION, output_filename)
    os.makedirs(os.path.dirname(outputpath), exist_ok=True)
    try:
        nrows, nmatched = match_rows(client, inputpath, outputpath + '.tmp',
            bigqueryized_header, countryfieldlist)
        os.rename(outputpath + '.tmp', outputpath)
    finally:
        if os.path.exists(outputpath + '.tmp'):
            os.remove(outputpath + '.tmp')

    if outputurl is None:
        output_url_list = [os.path.abspath(outputpath)]
    else:
        output_url_list = [f'{outputurl.rstrip("/")}/{OUTPUT_LOCATION}/{output_filename}']

    try:
        job.send_email(email, output_url_list, mailer)
    except Exception as e:
        logging.error(f'Error sending email: {e}. Files stored at {output_url_list}')

    elapsedtime = time.perf_counter()-starttime
    s = f'Success! Total elapsed time = {elapsedtime:1.3f}s, '
    s += f'{nrows} rows, {nmatched} matched\n'
    s += f'Output to {output_url_list} for job:\n'
    s += f'{json.dumps(json_config_in)}'
    logging.info(s)
    return output_url_list

def run_next_job(bulkdir, client, mailer, outputurl=None):
    ''' Claim the oldest queued job, run it, and move it to done or failed.
    returns:
        the job file name, or None if the queue was empty
    '''
    queuedir = os.path.join(bulkdir, 'jobs', 'queue')
    queued = sorted((f for f in os.listdir(queuedir) if f.endswith('.json')),
        key=lambda f: os.path.getmtime(os.path.join(queuedir, f)))
    for name in queued:
        running = os.path.join(bulkdir, 'jobs', 'running', name)
        try:
            # The rename claims the job; another worker that loses the race skips it.
            os.rename(os.path.join(queuedir, name), running)
        except FileNotFoundError:
            continue
        state = 'done'
        try:
            with open(running, encoding='utf-8') as f:
                json_config_in = json.load(f)
            logging.info(f'New job: {json_config_in}')
            process_csv_in_bulk_pg(json_config_in, client, bulkdir, mailer, outputurl)
        except Exception as e:
            state = 'failed'
            logging.exception(f'Job {name} failed: {e}')
        os.rename(running, os.path.join(bulkdir, 'jobs', state, name))
        return name
    return None

def main():
    parser = argparse.ArgumentParser(description='Run queued BELS bulk jobs with PostgreSQL.')
    parser.add_argument('--once', action='store_true',
        help='run the queued jobs, then exit (default: keep watching the queue)')
    parser.add_argument('--poll', type=float, default=5,
        help='seconds between checks of an empty queue (default 5)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    bulkdir = bulk_dir()
    if bulkdir is None:
        logging.error('BELS_BULK_DIR is not set.')
        return 1
    for state in JOB_STATES:
        os.makedirs(os.path.join(bulkdir, 'jobs', state), exist_ok=True)
    client = PG_BELS_Client()
    client.populate()
    mailer = get_mailer(bulkdir)
    outputurl = os.getenv('BELS_BULK_URL')
    while True:
        if run_next_job(bulkdir, client, mailer, outputurl) is None:
            if args.once:
                return 0
            time.sleep(args.poll)

if __name__ == '__main__':
    sys.exit(main())
