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
__filename__ = "parity_full.py"
__version__ = __filename__ + ' ' + "2026-10-26T15:30-07:00"

# The parity check of parity_tests.py for a full snapshot export: for every row of
# every snapshot file, rebuild the three match strings with the Python code and
# compare them with the stored strings, with the same rules and known exceptions
# (including the HerpNet sans coords strings, which are counted separately).
#
# Files are checked in parallel. For each file, the mismatches are written to
# <output dir>/<file name>.tsv (table, base64 dwc_location_hash, match string column,
# stored string, Python string), and the numbers of strings compared, mismatches and
# HerpNet sans coords strings to <file name>.done. A file with a .done result is skipped, so an interrupted run can
# be started again. At the end, a summary of all files is printed.
#
# Example:
#
# PYTHONPATH=../bels python parity_full.py /path/to/export/snapshot parity_out --jobs 32

import argparse
import base64
import concurrent.futures
import csv
import os
import sys

import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'postgres'))
from load_snapshot import snapshot_files
from id_utils import location_match_str
from id_utils import super_simplify
from parity_tests import KNOWN_EXCEPTIONS
from parity_tests import herpnet_sans_coords_str
from parity_tests import MATCHSTRINGS
from parity_tests import rebuild_input

def check_file(path, table, outdir):
    ''' Compare the stored match strings of one snapshot file with the Python strings
        and write the result files.
    parameters:
        path - path of the Parquet file
        table - name of the best georeference table
        outdir - output directory
    returns:
        (compared, mismatches, herpnet) - numbers of strings compared, not equal, and
            equal only as a HerpNet sans coords string
    '''
    name = os.path.basename(path)
    compared = 0
    mismatches = 0
    herpnet = 0
    tmppath = os.path.join(outdir, name + '.tsv.part')
    with open(tmppath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        parquetfile = pq.ParquetFile(path)
        for batch in parquetfile.iter_batches(batch_size=10000):
            for row in batch.to_pylist():
                loc = rebuild_input(row)
                hashid = None
                for column, termlist in MATCHSTRINGS:
                    compared += 1
                    matchstr = super_simplify(location_match_str(termlist, loc))
                    if matchstr == row[column]:
                        continue
                    if (column == 'matchme_sans_coords' and
                            herpnet_sans_coords_str(row, loc) == row[column]):
                        herpnet += 1
                        continue
                    if hashid is None:
                        hashid = base64.b64encode(row['dwc_location_hash']).decode('utf-8')
                    if (table, hashid, column) in KNOWN_EXCEPTIONS:
                        continue
                    mismatches += 1
                    writer.writerow([table, hashid, column, row[column], matchstr])
    os.replace(tmppath, os.path.join(outdir, name + '.tsv'))
    with open(os.path.join(outdir, name + '.done'), 'w') as f:
        f.write(f'{compared}\t{mismatches}\t{herpnet}\n')
    return compared, mismatches, herpnet

def main():
    parser = argparse.ArgumentParser(description='Full-snapshot match string parity check.')
    parser.add_argument('snapshot_dir', help='directory with the snapshot Parquet files')
    parser.add_argument('output_dir', help='directory for the result files')
    parser.add_argument('--jobs', type=int, default=1, help='files to check at a time')
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    tablefiles, countryfile = snapshot_files(args.snapshot_dir)
    tasks = []
    for table, paths in tablefiles.items():
        for path in paths:
            if not os.path.exists(os.path.join(args.output_dir,
                                               os.path.basename(path) + '.done')):
                tasks.append((path, table))
    total = sum(len(paths) for paths in tablefiles.values())
    print(f'{total} files, {total - len(tasks)} already checked', flush=True)

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.jobs) as executor:
        futures = {executor.submit(check_file, path, table, args.output_dir): path
                   for path, table in tasks}
        for n, future in enumerate(concurrent.futures.as_completed(futures), 1):
            compared, mismatches, herpnet = future.result()
            print(f'{n}/{len(tasks)} {os.path.basename(futures[future])}: '
                  f'{compared} strings, {mismatches} mismatches, {herpnet} HerpNet',
                  flush=True)

    # Summary over all files, including those checked in earlier runs
    for table, paths in tablefiles.items():
        compared = 0
        herpnet = 0
        mismatches = {column: 0 for column, termlist in MATCHSTRINGS}
        for path in paths:
            name = os.path.basename(path)
            with open(os.path.join(args.output_dir, name + '.done')) as f:
                counts = f.read().split('\t')
                compared += int(counts[0])
                herpnet += int(counts[2])
            with open(os.path.join(args.output_dir, name + '.tsv'), newline='',
                      encoding='utf-8') as f:
                for row in csv.reader(f, delimiter='\t'):
                    mismatches[row[2]] += 1
        print(f'{table}: {compared} strings compared, {sum(mismatches.values())} '
              f'mismatches {mismatches}, {herpnet} HerpNet sans coords strings')
    return 0

if __name__ == '__main__':
    sys.exit(main())
