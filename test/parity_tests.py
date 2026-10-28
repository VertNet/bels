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
__filename__ = "parity_tests.py"
__version__ = __filename__ + ' ' + "2026-10-20T14:35-07:00"

# This file tests that the Python match strings used by the API reproduce the match
# strings stored in the gazetteer, which were built in BigQuery (gazetteer/load_*.sql).
# For every row of the snapshot tables, it rebuilds the three match strings from the
# verbatim (v_) fields with interpreted_countrycode as countrycode, as BigQuery did.
#
# The snapshot directory can be changed with the environment variable
# BELS_SNAPSHOT_DIR, for example to run against a full snapshot export.
#
# Example:
#
# python parity_tests.py

import base64
import os
import unittest

import pyarrow.parquet as pq

from id_utils import location_match_str
from id_utils import super_simplify
from dwca_terms import locationmatchwithcoordstermlist
from dwca_terms import locationmatchverbatimcoordstermlist
from dwca_terms import locationmatchsanscoordstermlist

# Match string column and the terms used to rebuild it
MATCHSTRINGS = [
    ('matchme_with_coords', locationmatchwithcoordstermlist),
    ('matchme', locationmatchverbatimcoordstermlist),
    ('matchme_sans_coords', locationmatchsanscoordstermlist)
    ]

TABLES = [
    'matchme_with_coords_best_georef',
    'matchme_verbatimcoords_best_georef',
    'matchme_sans_coords_best_georef'
    ]

# Stored strings that the current build scripts would not produce. These are left as
# they are until the next snapshot.
# (table, base64 dwc_location_hash, match string column): reason
KNOWN_EXCEPTIONS = {}

def rebuild_input(row):
    ''' Get the input for location_match_str() from a stored gazetteer row.
    parameters:
        row - dict of one row of a best georeference table
    returns:
        loc - dict of the verbatim fields without the v_ prefix, with countrycode
              set to interpreted_countrycode
    '''
    loc = {}
    for key, value in row.items():
        if key.startswith('v_') and value is not None:
            loc[key[2:]] = value
    loc['countrycode'] = row['interpreted_countrycode']
    return loc

def herpnet_sans_coords_str(row, loc):
    ''' Get the sans coords match string of a HerpNet row as gazetteer/load_vertnet.sql
        built it. For HerpNet (temp_locations_herpnet_distinct), for_match_sans_coords
        always concatenates locality and verbatimlocality, without the check
        upper(v_locality)=upper(v_verbatimlocality) used for every other string, so
        the stored string repeats the locality when the two are equal. An API request
        does not tell the source, so location_match_str() can not build this string.
    parameters:
        row - dict of one row of a best georeference table
        loc - the input from rebuild_input(row)
    returns:
        str - the match string, or None if the row is not from HerpNet
    '''
    if row['source'] != 'HerpNet':
        return None
    loc = dict(loc)
    # Join with a space, as CONCAT(v_locality,' ',v_verbatimlocality) and as
    # location_match_str() separates terms: the separator changes the result, e.g. a
    # period before a space is removed but a period before a digit is kept
    if loc.get('verbatimlocality') is not None:
        locality = loc.get('locality')
        vlocality = loc.pop('verbatimlocality')
        loc['locality'] = vlocality if locality is None else locality + ' ' + vlocality
    return super_simplify(location_match_str(locationmatchsanscoordstermlist, loc))

class ParityTestCase(unittest.TestCase):
    # snapshotpath is the location of the snapshot Parquet files to test with
    snapshotpath = os.getenv('BELS_SNAPSHOT_DIR', '../bels_dev_snapshot/')

    def test_snapshot_files_exist(self):
        print('Running test_snapshot_files_exist')
        for table in TABLES:
            path = os.path.join(self.snapshotpath, table + '.parquet')
            self.assertTrue(os.path.isfile(path), f'{path} does not exist')

    def test_match_strings(self):
        print('Running test_match_strings')
        compared = 0
        herpnet = 0
        mismatches = []
        for table in TABLES:
            path = os.path.join(self.snapshotpath, table + '.parquet')
            parquetfile = pq.ParquetFile(path)
            for batch in parquetfile.iter_batches(batch_size=10000):
                for row in batch.to_pylist():
                    loc = rebuild_input(row)
                    hashid = base64.b64encode(row['dwc_location_hash']).decode('utf-8')
                    for column, termlist in MATCHSTRINGS:
                        compared += 1
                        matchstr = super_simplify(location_match_str(termlist, loc))
                        if matchstr == row[column]:
                            continue
                        if (column == 'matchme_sans_coords' and
                                herpnet_sans_coords_str(row, loc) == row[column]):
                            herpnet += 1
                            continue
                        if (table, hashid, column) in KNOWN_EXCEPTIONS:
                            continue
                        mismatches.append(
                            f'{table} {hashid} {column}\n'
                            f'  stored: {row[column]!r}\n  python: {matchstr!r}')
        print(f'Compared {compared} match strings, {len(mismatches)} mismatches, '
              f'{herpnet} HerpNet sans coords strings')
        s = '\n'.join(mismatches[:20])
        self.assertEqual(len(mismatches), 0, s)

if __name__ == '__main__':
    print('=== parity_tests.py ===')
    unittest.main()
