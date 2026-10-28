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
__filename__ = "bulk_match_tests.py"
__version__ = __filename__ + ' ' + "2026-10-21T16:06-07:00"

# This file tests bels/bulk_match.py, the bulk path match strings. The expected values
# of the JavaScript cases were produced by running the function in
# gazetteer/udf_matchString.sql in Node.js (V8, the engine of BigQuery JavaScript
# functions).
#
# It also checks the bulk strings of the snapshot rows against the stored gazetteer
# strings. They differ where the gazetteer build (gazetteer/load_*.sql) and matchString
# differ: the gazetteer build joins fields with a space and rounds coordinates to 9 and
# then 7 places; matchString joins them with nothing and rounds once, halves toward
# +Infinity. Every difference must be one of these.
#
# Example:
#
# python bulk_match_tests.py

import base64
import os
import unittest

import pyarrow.parquet as pq

import bulk_match as bm
from id_utils import bigquery_coordinate_str
from id_utils import super_simplify
from parity_tests import KNOWN_EXCEPTIONS
from parity_tests import herpnet_sans_coords_str
from parity_tests import rebuild_input

SNAPSHOTPATH = os.getenv('BELS_SNAPSHOT_DIR', '../bels_dev_snapshot/')

# (decimallatitude string, isNumber(), String(roundToSeven())) from Node.js
COORDINATE_CASES = [
    ('45.12345675', True, '45.1234568'),
    ('-45.12345675', True, '-45.1234567'),
    ('45.12345685', True, '45.1234569'),
    (' 45.1', True, '45.1'),
    ('45.1 ', True, 'NaN'),
    ('0x1A', True, 'NaN'),
    ('1e3', True, 'NaN'),
    ('Infinity', True, 'NaN'),
    ('-0.00000004', True, '0'),
    ('0.0000001', True, '1e-7'),
    ('-0.00000005', True, '0'),
    ('180', True, '180'),
    ('12abc', False, 'NaN'),
    ('.5', True, '0.5'),
    ('5.', True, '5'),
    (' 45.5', True, '45.5'),
    ]

# (row, [matchString(row, withcoords), verbatimcoords, sanscoords]) from Node.js
ROW_CASES = [
    ({'bels_interpreted_countrycode': 'MZ', 'stateprovince': 'Gaza',
      'locality': 'Arredores do Chongoene.', 'verbatimlocality': 'Xai - Xai, Chongoene..',
      'minimumelevationinmeters': '31', 'decimallatitude': '-25.020278',
      'decimallongitude': '33.795850'},
     ['MZGazaarredores do chongoene.xai - xai, chongoene..31-25.02027833.79585',
      'MZGazaarredores do chongoene.xai - xai, chongoene..31',
      'MZGazaarredores do chongoene.xai - xai, chongoene..31']),
    ({'bels_interpreted_countrycode': 'US', 'locality': ' Foo ', 'verbatimlocality': 'FOO'},
     ['USfoo', 'USfoo', 'USfoo']),
    ({'bels_interpreted_countrycode': 'US', 'locality': 'Foo', 'verbatimlocality': 'Bar ',
      'county': ' Kent '},
     ['US Kent foobar', 'US Kent foobar', 'US Kent foobar']),
    ({'bels_interpreted_countrycode': None, 'locality': '  ', 'verbatimlocality': None,
      'decimallatitude': '45.1 ', 'decimallongitude': '10'},
     ['NaN10', '', '']),
    ({'bels_interpreted_countrycode': 'AR', 'locality': 'Rt 1',
      'verbatimcoordinates': '28S 65W', 'decimallatitude': '0x1A',
      'decimallongitude': '-0.00000004'},
     ['ARrt 128S 65WNaN0', 'ARrt 128S 65W', 'ARrt 1']),
    ]

def gazetteer_style_match_str(row, matchtype, separator, gazetteer_rounding):
    ''' matchString with either difference from the gazetteer build switched on: a space
        between fields, and the gazetteer's rounding of coordinates.
    '''
    sep = ' ' if separator else ''
    parts = [row[f] for f in bm.GEOGFIELDS if bm.has_value(row, f)]
    locval = ''
    vlocval = ''
    if bm.has_value(row, 'locality'):
        locval = bm.js_trim(row['locality']).lower()
    if bm.has_value(row, 'verbatimlocality'):
        vlocval = bm.js_trim(row['verbatimlocality']).lower()
    parts.append(locval if locval == vlocval else bm.js_trim(locval + sep + vlocval))
    parts += [row[f] for f in bm.ELEVDEPTHFIELDS if bm.has_value(row, f)]
    if matchtype in ['verbatimcoords', 'withcoords']:
        parts += [row[f] for f in bm.VCOORDSFIELDS if bm.has_value(row, f)]
    if matchtype == 'withcoords':
        lat = row.get('decimallatitude')
        lng = row.get('decimallongitude')
        if bm.has_value(row, 'decimallatitude') and bm.js_is_number(lat) and \
            bm.has_value(row, 'decimallongitude') and bm.js_is_number(lng):
            for value in [lat, lng]:
                if gazetteer_rounding:
                    parts.append(bigquery_coordinate_str(value))
                else:
                    parts.append(bm.js_number_to_string(bm.js_round_to_seven(value)))
    return super_simplify(sep.join(parts))

class BulkMatchTestCase(unittest.TestCase):
    def test_js_primitives(self):
        print('Running test_js_primitives')
        self.assertEqual(bm.js_trim('﻿　 a b \n'), 'a b')
        self.assertEqual(bm.js_trim('\x1ca\x85'), '\x1ca\x85')
        self.assertEqual(bm.js_to_number(' 12 '), 12.0)
        self.assertEqual(bm.js_to_number('0x10'), 16.0)
        self.assertNotEqual(bm.js_to_number('-0x10'), bm.js_to_number('-0x10'))
        self.assertEqual(bm.js_to_number(''), 0.0)
        self.assertEqual(bm.js_parse_float('0x10'), 0.0)
        self.assertEqual(bm.js_parse_float(' 1.5e3abc'), 1500.0)
        self.assertEqual(bm.js_math_round(2.5), 3.0)
        self.assertEqual(bm.js_math_round(-2.5), -2.0)
        self.assertEqual(bm.js_math_round(0.49999999999999994), 0.0)
        self.assertEqual(bm.js_number_to_string(1e21), '1e+21')
        self.assertEqual(bm.js_number_to_string(1e20), '100000000000000000000')
        self.assertEqual(bm.js_number_to_string(0.000001), '0.000001')
        self.assertEqual(bm.js_number_to_string(1.5e-7), '1.5e-7')
        self.assertEqual(bm.js_number_to_string(-0.0), '0')

    def test_coordinates(self):
        print('Running test_coordinates')
        for value, isnumber, rounded in COORDINATE_CASES:
            self.assertEqual(bm.js_is_number(value), isnumber, value)
            self.assertEqual(bm.js_number_to_string(bm.js_round_to_seven(value)),
                rounded, value)

    def test_match_string(self):
        print('Running test_match_string')
        for row, expected in ROW_CASES:
            self.assertEqual([bm.match_string(row, t) for t in bm.MATCHTYPES], expected,
                row)

    def test_snapshot_differences_explained(self):
        print('Running test_snapshot_differences_explained')
        columns = {'withcoords': 'matchme_with_coords', 'verbatimcoords': 'matchme',
            'sanscoords': 'matchme_sans_coords'}
        counts = {}
        unexplained = []
        for table in ['matchme_with_coords_best_georef',
            'matchme_verbatimcoords_best_georef', 'matchme_sans_coords_best_georef']:
            path = os.path.join(SNAPSHOTPATH, table + '.parquet')
            for r in pq.read_table(path).to_pylist():
                row = {k[2:]: v for k, v in r.items() if k.startswith('v_')}
                row['bels_interpreted_countrycode'] = r['interpreted_countrycode']
                for matchtype, column in columns.items():
                    stored = r[column]
                    if bm.bulk_match_str(row, matchtype) == stored:
                        outcome = 'equal'
                    elif gazetteer_style_match_str(row, matchtype, True, False) == stored:
                        outcome = 'separator'
                    elif gazetteer_style_match_str(row, matchtype, False, True) == stored:
                        outcome = 'rounding'
                    elif gazetteer_style_match_str(row, matchtype, True, True) == stored:
                        outcome = 'separator and rounding'
                    elif (column == 'matchme_sans_coords' and
                            herpnet_sans_coords_str(r, rebuild_input(r)) == stored):
                        outcome = 'HerpNet sans coords'
                    else:
                        outcome = 'unexplained'
                        hashid = base64.b64encode(r['dwc_location_hash']).decode('utf-8')
                        if (table, hashid, column) in KNOWN_EXCEPTIONS:
                            outcome = 'known exception'
                        else:
                            unexplained.append(f'{table} {column}: {stored!r}')
                    counts[outcome] = counts.get(outcome, 0) + 1
        print(f'Bulk strings vs stored strings: {counts}')
        self.assertEqual(unexplained, [], '\n'.join(unexplained[:10]))

if __name__ == '__main__':
    print('=== bulk_match_tests.py ===')
    unittest.main()
