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
__filename__ = "load_snapshot.py"
__version__ = __filename__ + ' ' + "2026-10-26T15:30-07:00"

"""
Load a BELS gazetteer snapshot into PostgreSQL.

A snapshot is a directory with the BigQuery Parquet exports of the three best
georeference tables plus the countrycode_lookup CSV. Each table is either one file
(as in bels_dev_snapshot) or the numbered files of a BigQuery export, with or
without the .parquet extension:
    matchme_with_coords_best_georef.parquet
      or matchme_with_coords_best_georef000000000000, ...000000000001, ...
    matchme_verbatimcoords_best_georef.parquet (or numbered files)
    matchme_sans_coords_best_georef.parquet (or numbered files)
    countrycode_lookup.csv (or countrycode_lookup)

Steps:
  1. Create the staging schema (default gazetteer_next) with the BELS runtime DDL
     (bels_runtime_ddl_postgresql.sql), run through psql, and drop its lookup indexes
     so they can be built after the load.
  2. COPY each Parquet file into the staging schema, and the country CSV into
     vocabs.countrycode_lookup_next.
  3. Run the DDL again, which now only builds the lookup indexes, then ANALYZE.
  4. Verify: row counts, and every loaded value against the Parquet files.
  5. With --swap, publish the staging schema as gazetteer and replace
     vocabs.countrycode_lookup, in one transaction.

Each file is loaded, and later verified, in its own transaction, which also records
it in the staging table load_progress. If a load is interrupted, run it again with
--resume to keep the staging schema and skip the files already done. --jobs loads
and verifies that many files at a time, each with its own connection. load_progress
is dropped when the staging schema is published.

Connection settings come from the standard PostgreSQL environment variables
(PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD) or --dsn. The psql program is
taken from the environment variable PSQL, or psql on the PATH.

Example:
    PGDATABASE=localityservice python load_snapshot.py \\
        --ddl bels_runtime_ddl_postgresql.sql --swap ../bels_dev_snapshot
"""

import argparse
import concurrent.futures
import logging
import multiprocessing
import os
import re
import subprocess
import sys
import time

import psycopg
from psycopg import sql
import pyarrow.parquet as pq

TABLES = [
    ('matchme_with_coords_best_georef', 'matchme_with_coords'),
    ('matchme_verbatimcoords_best_georef', 'matchme'),
    ('matchme_sans_coords_best_georef', 'matchme_sans_coords')
    ]
COUNTRY_FILES = ['countrycode_lookup.csv', 'countrycode_lookup']
LIVE_SCHEMA = 'gazetteer'
PREV_SCHEMA = 'gazetteer_prev'
PROGRESS_TABLE = 'load_progress'
BATCH_SIZE = 10000

def snapshot_files(snapshot_dir):
    ''' Find the files of a snapshot.
    parameters:
        snapshot_dir - the snapshot directory
    returns:
        (tablefiles, countryfile) - dict of table name: sorted list of Parquet file
            paths (empty if none found), and the path of the country CSV (None if
            not found)
    '''
    names = sorted(os.listdir(snapshot_dir))
    tablefiles = {}
    for table, key in TABLES:
        if table + '.parquet' in names:
            found = [table + '.parquet']
        else:
            pattern = re.compile(re.escape(table) + r'\d{12}(\.parquet)?')
            found = [n for n in names if pattern.fullmatch(n)]
        tablefiles[table] = [os.path.join(snapshot_dir, n) for n in found]
    countryfile = None
    for name in COUNTRY_FILES:
        if name in names:
            countryfile = os.path.join(snapshot_dir, name)
            break
    return tablefiles, countryfile

def run_ddl(ddlfile, schema, dsn):
    ''' Run the BELS runtime DDL through psql for the given gazetteer schema.
    parameters:
        ddlfile - path to bels_runtime_ddl_postgresql.sql
        schema - value for the psql variable gazetteer_schema
        dsn - connection string, or '' to use the PG* environment variables
    returns:
        None
    '''
    command = [os.getenv('PSQL', 'psql'), '-X', '-q', '-v', 'ON_ERROR_STOP=1',
               '-v', f'gazetteer_schema={schema}', '-f', ddlfile]
    if dsn:
        command += ['-d', dsn]
    # Hide the notices for objects that already exist
    env = dict(os.environ, PGOPTIONS='-c client_min_messages=warning')
    subprocess.run(command, check=True, env=env)

def table_columns(conn, schema, table):
    ''' Get the column names and types of a table, in table order.
    parameters:
        conn - an open psycopg connection
        schema - schema name
        table - table name
    returns:
        list of (column_name, data_type) tuples
    '''
    rows = conn.execute("""
SELECT column_name, udt_name
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position""", (schema, table)).fetchall()
    return rows

def prepare_staging(conn, ddlfile, schema, dsn):
    ''' Create an empty staging schema from the DDL, without its lookup indexes, and
        an empty vocabs.countrycode_lookup_next.
    parameters:
        conn - an open psycopg connection
        ddlfile - path to the DDL file
        schema - staging schema name
        dsn - connection string for psql
    returns:
        None
    '''
    if schema == LIVE_SCHEMA:
        raise ValueError(f'The staging schema can not be {LIVE_SCHEMA}.')
    conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
    conn.commit()
    run_ddl(ddlfile, schema, dsn)
    # Indexes are much faster to build after the load
    for table, key in TABLES:
        conn.execute(sql.SQL('DROP INDEX IF EXISTS {}').format(
            sql.Identifier(schema, f'{table}_key_hidx')))
    conn.execute('DROP TABLE IF EXISTS vocabs.countrycode_lookup_next')
    conn.execute("""CREATE TABLE vocabs.countrycode_lookup_next
  (LIKE vocabs.countrycode_lookup INCLUDING ALL)""")
    conn.execute(sql.SQL("""CREATE TABLE {}
  (step text, file text, rows bigint, finished timestamptz DEFAULT now(),
   PRIMARY KEY (step, file))""").format(sql.Identifier(schema, PROGRESS_TABLE)))
    conn.commit()

def done_files(conn, schema):
    ''' Get the files already loaded or verified into a staging schema.
    parameters:
        conn - an open psycopg connection
        schema - staging schema name
    returns:
        set of (step, file name) tuples, or None if the schema has no load_progress
    '''
    exists = conn.execute('SELECT to_regclass(%s)',
                          (f'"{schema}".{PROGRESS_TABLE}',)).fetchone()[0]
    if exists is None:
        conn.rollback()
        return None
    rows = conn.execute(sql.SQL('SELECT step, file FROM {}').format(
        sql.Identifier(schema, PROGRESS_TABLE))).fetchall()
    conn.rollback()
    return set(rows)

def record_done(conn, schema, step, path, rows):
    ''' Record a loaded or verified file in load_progress, in the open transaction.
    parameters:
        conn - an open psycopg connection
        schema - staging schema name
        step - 'load' or 'verify'
        path - path of the file
        rows - number of rows loaded or verified
    returns:
        None
    '''
    conn.execute(sql.SQL('INSERT INTO {} (step, file, rows) VALUES (%s, %s, %s)').format(
        sql.Identifier(schema, PROGRESS_TABLE)), (step, os.path.basename(path), rows))

def check_schema(parquetfile, columns, table):
    ''' Make sure the Parquet file has the same columns, in the same order, as the
        table.
    parameters:
        parquetfile - a pyarrow ParquetFile
        columns - list of (column_name, data_type) for the table
        table - table name, for messages
    returns:
        None
    '''
    names = parquetfile.schema_arrow.names
    expected = [c for c, t in columns]
    if names != expected:
        missing = [c for c in expected if c not in names]
        extra = [c for c in names if c not in expected]
        raise ValueError(f'{table}: Parquet columns do not match the table. '
                         f'Missing: {missing} Extra: {extra}')

def to_copy_value(value, datatype):
    ''' Convert a value from Parquet to what COPY expects for the column type.
    parameters:
        value - the value read from Parquet
        datatype - the PostgreSQL type name of the column
    returns:
        the value to write
    '''
    if value is None:
        return None
    if datatype == 'geography':
        # BigQuery exports GEOGRAPHY as WKB, which PostGIS reads as hex
        return value.hex()
    if datatype == 'text' and '\x00' in value:
        # PostgreSQL text can not hold NUL, and dropping it would change the data
        raise ValueError(f'Value contains a NUL character: {value!r}')
    return value

def load_table(conn, parquetpath, schema, table):
    ''' COPY one Parquet file into a table and record it in load_progress, in one
        transaction.
    parameters:
        conn - an open psycopg connection
        parquetpath - path to the Parquet file
        schema - schema name
        table - table name
    returns:
        rowcount - number of rows copied
    '''
    columns = table_columns(conn, schema, table)
    parquetfile = pq.ParquetFile(parquetpath)
    check_schema(parquetfile, columns, table)
    names = [c for c, t in columns]
    types = [t for c, t in columns]
    copystatement = sql.SQL('COPY {} ({}) FROM STDIN').format(
        sql.Identifier(schema, table), sql.SQL(', ').join(map(sql.Identifier, names)))
    rowcount = 0
    with conn.cursor() as cursor:
        with cursor.copy(copystatement) as copy:
            for batch in parquetfile.iter_batches(batch_size=BATCH_SIZE):
                for row in zip(*[col.to_pylist() for col in batch.columns]):
                    copy.write_row([to_copy_value(v, t) for v, t in zip(row, types)])
                rowcount += batch.num_rows
    record_done(conn, schema, 'load', parquetpath, rowcount)
    conn.commit()
    return rowcount

def load_countries(conn, csvpath, schema):
    ''' COPY the country CSV into vocabs.countrycode_lookup_next and record it in
        load_progress, in one transaction. As in a BigQuery CSV load, empty fields
        become NULL.
    parameters:
        conn - an open psycopg connection
        csvpath - path to the CSV file
        schema - staging schema name, for load_progress
    returns:
        rowcount - number of rows copied
    '''
    with conn.cursor() as cursor:
        with cursor.copy("""COPY vocabs.countrycode_lookup_next (u_country, countrycode)
FROM STDIN WITH (FORMAT csv, HEADER true)""") as copy:
            with open(csvpath, 'rb') as f:
                while data := f.read(1 << 20):
                    copy.write(data)
        rowcount = cursor.rowcount
    record_done(conn, schema, 'load', csvpath, rowcount)
    conn.commit()
    return rowcount

def build_indexes(conn, ddlfile, schema, dsn):
    ''' Build the lookup indexes (the DDL skips the tables, which already exist), then
        update planner statistics.
    parameters:
        conn - an open psycopg connection
        ddlfile - path to the DDL file
        schema - staging schema name
        dsn - connection string for psql
    returns:
        None
    '''
    run_ddl(ddlfile, schema, dsn)
    conn.autocommit = True
    for table, key in TABLES:
        conn.execute(sql.SQL('ANALYZE {}').format(sql.Identifier(schema, table)))
    conn.execute('ANALYZE vocabs.countrycode_lookup_next')
    conn.autocommit = False

def verify_table(conn, parquetpath, schema, table, key, limit=None):
    ''' Compare the loaded rows with the Parquet file, value by value, by lookup key.
        If there are no problems, record the file in load_progress.
    parameters:
        conn - an open psycopg connection
        parquetpath - path to the Parquet file
        schema - schema name
        table - table name
        key - the lookup key column, unique by construction
        limit - compare only the first limit rows of the Parquet file (None for all)
    returns:
        (checked, problems) - number of rows checked and a list of problem messages
    '''
    columns = table_columns(conn, schema, table)
    names = [c for c, t in columns]
    selectlist = [sql.SQL('ST_AsBinary({})').format(sql.Identifier(c)) if t == 'geography'
                  else sql.Identifier(c) for c, t in columns]
    query = sql.SQL('SELECT {} FROM {} WHERE {} = ANY(%s)').format(
        sql.SQL(', ').join(selectlist), sql.Identifier(schema, table), sql.Identifier(key))
    keyindex = names.index(key)
    checked = 0
    problems = []
    parquetfile = pq.ParquetFile(parquetpath)
    for batch in parquetfile.iter_batches(batch_size=1000):
        rows = batch.to_pylist()
        if limit is not None:
            rows = rows[:max(0, limit - checked)]
        if len(rows) == 0:
            break
        loaded = {}
        for dbrow in conn.execute(query, ([r[key] for r in rows],)):
            if dbrow[keyindex] in loaded:
                problems.append(f'{table}: {key} is not unique: {dbrow[keyindex]!r}')
            loaded[dbrow[keyindex]] = dbrow
        for row in rows:
            dbrow = loaded.get(row[key])
            if dbrow is None:
                problems.append(f'{table}: row not found for {key} {row[key]!r}')
                continue
            for name, dbvalue in zip(names, dbrow):
                if isinstance(dbvalue, memoryview):
                    dbvalue = bytes(dbvalue)
                if dbvalue != row[name]:
                    problems.append(f'{table}: {name} differs for {key} {row[key]!r}: '
                                    f'parquet {row[name]!r} postgres {dbvalue!r}')
        checked += len(rows)
    conn.rollback()
    if len(problems) == 0:
        record_done(conn, schema, 'verify', parquetpath, checked)
        conn.commit()
    return checked, problems

# Each worker process of --jobs keeps one connection, opened by init_worker
worker_conn = None

def init_worker(dsn):
    global worker_conn
    worker_conn = psycopg.connect(dsn)

def load_worker(path, schema, table):
    return path, load_table(worker_conn, path, schema, table)

def verify_worker(path, schema, table, key, limit):
    return (path,) + verify_table(worker_conn, path, schema, table, key, limit)

def run_parallel(function, tasks, dsn, jobs):
    ''' Run function over tasks in jobs worker processes, each with its own connection.
        On the first error, pending tasks are cancelled and the error is raised; files
        already committed stay in load_progress for --resume.
    parameters:
        function - load_worker or verify_worker
        tasks - list of argument tuples for function
        dsn - connection string, or '' to use the PG* environment variables
        jobs - number of worker processes
    returns:
        iterator of function results, in order of completion
    '''
    if len(tasks) == 0:
        return
    # spawn, so that no worker inherits the main process connection
    context = multiprocessing.get_context('spawn')
    with concurrent.futures.ProcessPoolExecutor(max_workers=min(jobs, len(tasks)),
            mp_context=context, initializer=init_worker, initargs=(dsn,)) as executor:
        futures = [executor.submit(function, *task) for task in tasks]
        try:
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
        except BaseException:
            for future in futures:
                future.cancel()
            raise

def swap(conn, schema):
    ''' Publish the staging schema as gazetteer and replace vocabs.countrycode_lookup,
        in one transaction. The previous gazetteer schema is kept as gazetteer_prev.
        load_progress is dropped, so the published schema has only the gazetteer tables.
    parameters:
        conn - an open psycopg connection
        schema - staging schema name
    returns:
        None
    '''
    with conn.transaction():
        conn.execute(sql.SQL('DROP TABLE IF EXISTS {}').format(
            sql.Identifier(schema, PROGRESS_TABLE)))
        conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
            sql.Identifier(PREV_SCHEMA)))
        exists = conn.execute('SELECT 1 FROM pg_namespace WHERE nspname = %s',
                              (LIVE_SCHEMA,)).fetchone()
        if exists:
            conn.execute(sql.SQL('ALTER SCHEMA {} RENAME TO {}').format(
                sql.Identifier(LIVE_SCHEMA), sql.Identifier(PREV_SCHEMA)))
        conn.execute(sql.SQL('ALTER SCHEMA {} RENAME TO {}').format(
            sql.Identifier(schema), sql.Identifier(LIVE_SCHEMA)))
        conn.execute('DELETE FROM vocabs.countrycode_lookup')
        conn.execute("""INSERT INTO vocabs.countrycode_lookup
  SELECT * FROM vocabs.countrycode_lookup_next""")
        conn.execute('DROP TABLE vocabs.countrycode_lookup_next')

def main():
    parser = argparse.ArgumentParser(description='Load a BELS snapshot into PostgreSQL.')
    parser.add_argument('snapshot_dir', help='directory with the Parquet and CSV files')
    parser.add_argument('--ddl', required=True, help='bels_runtime_ddl_postgresql.sql')
    parser.add_argument('--dsn', default='', help='connection string (default: PG* variables)')
    parser.add_argument('--schema', default='gazetteer_next', help='staging schema')
    parser.add_argument('--verify-limit', type=int, default=None,
                        help='verify only the first N rows of each file (default: all)')
    parser.add_argument('--swap', action='store_true',
                        help='publish the staging schema as gazetteer if verification passes')
    parser.add_argument('--resume', action='store_true',
                        help='keep the staging schema and skip the files already done')
    parser.add_argument('--jobs', type=int, default=1,
                        help='files to load and verify at a time (default: 1)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    tablefiles, countryfile = snapshot_files(args.snapshot_dir)
    for table, key in TABLES:
        if len(tablefiles[table]) == 0:
            logging.error(f'Missing snapshot files for {table} in {args.snapshot_dir}')
            return 1
    if countryfile is None:
        logging.error(f'Missing {COUNTRY_FILES[0]} in {args.snapshot_dir}')
        return 1

    with psycopg.connect(args.dsn) as conn:
        done = done_files(conn, args.schema) if args.resume else None
        if done is None:
            if args.resume:
                logging.info(f'Nothing to resume in {args.schema}')
            logging.info(f'Creating staging schema {args.schema}')
            prepare_staging(conn, args.ddl, args.schema, args.dsn)
            done = set()
        else:
            logging.info(f'Resuming {args.schema}: {len(done)} files already done')

        expected = {}
        for table, key in TABLES:
            starttime = time.perf_counter()
            expected[table] = sum(pq.ParquetFile(p).metadata.num_rows
                                  for p in tablefiles[table])
            tasks = [(p, args.schema, table) for p in tablefiles[table]
                     if ('load', os.path.basename(p)) not in done]
            count = 0
            for path, rows in run_parallel(load_worker, tasks, args.dsn, args.jobs):
                count += rows
                if len(tablefiles[table]) > 1:
                    logging.info(f'Loaded {rows} rows from {os.path.basename(path)}')
            logging.info(f'Loaded {count} rows from {len(tasks)} files into '
                         f'{args.schema}.{table} in {time.perf_counter()-starttime:1.1f}s '
                         f'({len(tablefiles[table]) - len(tasks)} files already loaded)')
        if ('load', os.path.basename(countryfile)) not in done:
            count = load_countries(conn, countryfile, args.schema)
            logging.info(f'Loaded {count} rows into vocabs.countrycode_lookup_next')

        starttime = time.perf_counter()
        build_indexes(conn, args.ddl, args.schema, args.dsn)
        logging.info(f'Built indexes in {time.perf_counter()-starttime:1.1f}s')

        problems = []
        for table, key in TABLES:
            count = conn.execute(sql.SQL('SELECT count(*) FROM {}').format(
                sql.Identifier(args.schema, table))).fetchone()[0]
            conn.rollback()
            if count != expected[table]:
                problems.append(f'{table}: {count} rows loaded, {expected[table]} in Parquet')
            starttime = time.perf_counter()
            tasks = [(p, args.schema, table, key, args.verify_limit) for p in tablefiles[table]
                     if ('verify', os.path.basename(p)) not in done]
            checked = 0
            tableproblems = []
            for path, filechecked, fileproblems in run_parallel(verify_worker, tasks,
                                                                args.dsn, args.jobs):
                checked += filechecked
                tableproblems += fileproblems
                if len(tablefiles[table]) > 1:
                    logging.info(f'Verified {filechecked} rows from {os.path.basename(path)}: '
                                 f'{len(fileproblems)} problems')
            problems += tableproblems
            logging.info(f'Verified {checked} rows of {table} from {len(tasks)} files: '
                         f'{len(tableproblems)} problems in '
                         f'{time.perf_counter()-starttime:1.1f}s '
                         f'({len(tablefiles[table]) - len(tasks)} files already verified)')
        for problem in problems[:50]:
            logging.error(problem)
        if problems:
            logging.error(f'Verification failed with {len(problems)} problems. '
                          f'{args.schema} was not published.')
            return 1

        if args.swap:
            swap(conn, args.schema)
            logging.info(f'Published {args.schema} as {LIVE_SCHEMA}')
        else:
            logging.info(f'Verification passed. Run with --swap to publish {args.schema}.')
    return 0

if __name__ == '__main__':
    sys.exit(main())
