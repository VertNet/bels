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
__filename__ = "bulk_match.py"
__version__ = __filename__ + ' ' + "2026-10-21T16:06-07:00"

"""
Match strings for the bulk path (/api/bels_csv), as built in BigQuery by
process_import_table() in bels_query.py:

    REGEXP_REPLACE(saveNumbers(NORMALIZE_AND_CASEFOLD(removeSymbols(simplifyDiacritics(
        matchString(TO_JSON_STRING(t), matchtype))),NFKC)),r"[\\s]+",'')

Everything after matchString is the same chain the gazetteer was built with, which
id_utils.super_simplify() reproduces. matchString (gazetteer/udf_matchString.sql) is a
JavaScript function, so it is reproduced here with the JavaScript rules it depends on:
String.prototype.trim(), Number() and parseFloat() on strings, Math.round() (halves
toward +Infinity) and Number to String conversion. This differs from the single-record
API (id_utils.location_match_str()), for example in rounding and in how locality and
verbatimlocality are combined.

The row is a dict of column name to string or None, as TO_JSON_STRING gives it for a
table of STRING columns.
"""

import math
import re
from decimal import Decimal

from id_utils import super_simplify

# Fields in the order of the objects in matchString
GEOGFIELDS = ['waterbody', 'islandgroup', 'island', 'bels_interpreted_countrycode',
    'stateprovince', 'county', 'municipality']
ELEVDEPTHFIELDS = ['minimumelevationinmeters', 'maximumelevationinmeters',
    'verbatimelevation', 'verticaldatum', 'minimumdepthinmeters', 'maximumdepthinmeters',
    'verbatimdepth']
VCOORDSFIELDS = ['verbatimcoordinates', 'verbatimlatitude', 'verbatimlongitude']
MATCHTYPES = ['withcoords', 'verbatimcoords', 'sanscoords']

# JavaScript WhiteSpace and LineTerminator characters, as removed by trim() and
# ignored around numbers by Number() (ECMAScript StrWhiteSpaceChar): tab, vertical tab,
# form feed, space, no-break space, zero width no-break space, the other Unicode Zs
# characters, LF, CR, line separator and paragraph separator.
JS_WHITESPACE = ('\t\x0b\x0c \xa0﻿       '
    '       　\n\r  ')

# ECMAScript StrDecimalLiteral, ASCII digits only
JS_DECIMAL = r'[+-]?(?:Infinity|(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)'
JS_DECIMAL_RE = re.compile(JS_DECIMAL)
JS_NONDECIMAL_RE = re.compile(r'0(?:[xX]([0-9a-fA-F]+)|[oO]([0-7]+)|[bB]([01]+))')

def js_trim(s):
    ''' JavaScript s.trim() '''
    return s.strip(JS_WHITESPACE)

def js_decimal_value(literal):
    ''' Get the Number for a string that matches JS_DECIMAL. '''
    if literal.lstrip('+-') == 'Infinity':
        return -math.inf if literal[0] == '-' else math.inf
    # Python parses the same decimal literals with the same correct rounding,
    # including overflow to infinity and the sign of zero.
    return float(literal)

def js_to_number(s):
    ''' JavaScript Number(s) for a string s (ECMAScript StringToNumber).
    returns:
        value - a float, math.nan if s is not a number
    '''
    t = js_trim(s)
    if t == '':
        return 0.0
    if JS_DECIMAL_RE.fullmatch(t):
        return js_decimal_value(t)
    m = JS_NONDECIMAL_RE.fullmatch(t)
    if m:
        for digits, base in zip(m.groups(), [16, 8, 2]):
            if digits is not None:
                try:
                    return float(int(digits, base))
                except OverflowError:
                    return math.inf
    return math.nan

def js_parse_float(s):
    ''' JavaScript parseFloat(s) for a string s: the longest decimal literal at the start
        of s after leading whitespace.
    '''
    m = JS_DECIMAL_RE.match(s.lstrip(JS_WHITESPACE))
    if m is None:
        return math.nan
    return js_decimal_value(m.group(0))

def js_is_number(s):
    ''' isNumber() in matchString: !isNaN(str) && !isNaN(parseFloat(str)) '''
    return not math.isnan(js_to_number(s)) and not math.isnan(js_parse_float(s))

def js_math_round(x):
    ''' JavaScript Math.round(x): the nearest integer, halves toward +Infinity, -0 for
        values in [-0.5, 0).
    '''
    if math.isnan(x) or math.isinf(x) or x == 0:
        return x
    if -0.5 <= x < 0:
        return -0.0
    f = math.floor(x)
    if x - f >= 0.5:
        return float(f + 1)
    return float(f)

def js_number_to_string(x):
    ''' JavaScript String(x) for a Number x (ECMAScript Number::toString, radix 10),
        using the shortest digits that round-trip, as Python repr() does.
    '''
    if math.isnan(x):
        return 'NaN'
    if x == 0:
        return '0'
    if x < 0:
        return '-' + js_number_to_string(-x)
    if math.isinf(x):
        return 'Infinity'
    sign, digittuple, exponent = Decimal(repr(x)).as_tuple()
    digits = ''.join(str(d) for d in digittuple)
    stripped = digits.rstrip('0')
    exponent += len(digits) - len(stripped)
    digits = stripped
    # x = digits * 10**exponent; k digits; n is the position of the decimal point
    k = len(digits)
    n = k + exponent
    if k <= n <= 21:
        return digits + '0'*(n-k)
    if 0 < n <= 21:
        return digits[:n] + '.' + digits[n:]
    if -6 < n <= 0:
        return '0.' + '0'*(-n) + digits
    e = n - 1
    esign = '+' if e >= 0 else '-'
    if k == 1:
        return f'{digits}e{esign}{abs(e)}'
    return f'{digits[0]}.{digits[1:]}e{esign}{abs(e)}'

def js_round_to_seven(value):
    ''' roundToSeven() in matchString, called with a string:
        +(Math.round(num + "e+7") + "e-7")
    '''
    rounded = js_math_round(js_to_number(value + 'e+7'))
    return js_to_number(js_number_to_string(rounded) + 'e-7')

def has_value(row, field):
    ''' obj.hasOwnProperty(field) && obj[field] != null && obj[field].trim() != "" '''
    value = row.get(field)
    return value is not None and js_trim(value) != ''

def match_string(row, matchtype):
    ''' The BigQuery function matchString(json_row, matchtype).
    parameters:
        row - dict of column name to string or None
        matchtype - one of 'withcoords', 'verbatimcoords', 'sanscoords'
    returns:
        match - the unsimplified match string
    '''
    functionname = 'match_string()'

    match = ''
    for field in GEOGFIELDS:
        if has_value(row, field):
            match += row[field]
    locval = ''
    vlocval = ''
    if has_value(row, 'locality'):
        locval = js_trim(row['locality']).lower()
    if has_value(row, 'verbatimlocality'):
        vlocval = js_trim(row['verbatimlocality']).lower()
    if locval == vlocval:
        match += locval
    else:
        match += js_trim(locval + vlocval)
    for field in ELEVDEPTHFIELDS:
        if has_value(row, field):
            match += row[field]
    if matchtype in ['verbatimcoords', 'withcoords']:
        for field in VCOORDSFIELDS:
            if has_value(row, field):
                match += row[field]
    if matchtype == 'withcoords':
        if has_value(row, 'decimallatitude') and js_is_number(row['decimallatitude']):
            if has_value(row, 'decimallongitude') and js_is_number(row['decimallongitude']):
                match += js_number_to_string(js_round_to_seven(row['decimallatitude']))
                match += js_number_to_string(js_round_to_seven(row['decimallongitude']))
    return match

def bulk_match_str(row, matchtype):
    ''' Get the bulk match string of a row, as process_import_table() builds it.
    parameters:
        row - dict of column name to string or None, including
            bels_interpreted_countrycode
        matchtype - one of 'withcoords', 'verbatimcoords', 'sanscoords'
    returns:
        the match string
    '''
    return super_simplify(match_string(row, matchtype))
