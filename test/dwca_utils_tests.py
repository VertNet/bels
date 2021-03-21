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

__author__ = "John Wieczorek"
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__version__ = "dwca_utils_tests.py 2021-03-21T16:02-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/test/dwca_utils_test.py"

# This file contains unit tests for the functions in dwca_utils.
#
# Example:
#
# python dwca_utils_test.py

from bels.dwca_utils import represents_int
from bels.dwca_utils import clean_header
from bels.dwca_utils import composite_header
from bels.dwca_utils import convert_csv
from bels.dwca_utils import count_rows
from bels.dwca_utils import csv_dialect
from bels.dwca_utils import csv_field_checker
from bels.dwca_utils import csv_file_dialect
from bels.dwca_utils import csv_file_encoding
from bels.dwca_utils import csv_file_dialect
from bels.dwca_utils import dialects_equal
from bels.dwca_utils import dwc_ordered_header
from bels.dwca_utils import extract_fields_from_row
from bels.dwca_utils import extract_value_counts_from_file
from bels.dwca_utils import extract_values_from_row
from bels.dwca_utils import extract_values_from_file
from bels.dwca_utils import get_guid
from bels.dwca_utils import header_map
from bels.dwca_utils import merge_headers
from bels.dwca_utils import purge_non_printing_from_file
from bels.dwca_utils import read_header
from bels.dwca_utils import split_path
from bels.dwca_utils import strip_list
from bels.dwca_utils import term_rowcount_from_file
from bels.dwca_utils import tsv_dialect
from bels.dwca_utils import ustripstr
from bels.dwca_utils import write_header
import os
import unittest
import csv

class DWCAUtilsTestFramework():
    # testdatapath is the location of the files to test with
    testdatapath = '../data/tests/'

    # following are files used as input during the tests, don't remove these
    non_printing_file = testdatapath + 'test_non-printing.csv'
    encodedfile_utf8 = testdatapath + 'test_eight_records_utf8_lf.csv'
    encodedfile_latin_1 = testdatapath + 'test_thirty_records_latin_1_crlf.csv'
    encodedfile_windows_1252 = testdatapath + 'test_bat_agave_data_idigbio.csv'

    symbiotafile = testdatapath + 'test_simbiota_download.csv'
    csvreadheaderfile = testdatapath + 'test_eight_specimen_records.csv'
    csvreadheaderfile2 = testdatapath + 'test_countrycodes_with_georefs_short.csv'
    tsvreadheaderfile = testdatapath + 'test_three_specimen_records.txt'
    tsvtest1 = testdatapath + 'test_tsv_1.txt'
    tsvtest2 = testdatapath + 'test_tsv_2.txt'
    csvtest1 = testdatapath + 'test_csv_1.csv'
    csvtest2 = testdatapath + 'test_csv_2.csv'
    csvtotsvfile1 = testdatapath + 'test_csv_1.csv'
    csvtotsvfile2 = testdatapath + 'test_csv_2.csv'
    monthvocabfile = testdatapath + 'test_vocab_month.txt'
    geogvocabfile = testdatapath + 'test_geography.txt'
    compositetestfile = testdatapath + 'test_eight_specimen_records.csv'
    fieldcountestfile1 = testdatapath + 'test_fieldcount.csv'
    fieldcountestfile2 = testdatapath + 'test_eight_specimen_records.csv'
    fieldcountestfile3 = testdatapath + 'test_bad_fieldcount1.txt'
    termrowcountfile1 = testdatapath + 'test_eight_specimen_records.csv'
    termrowcountfile2 = testdatapath + 'test_three_specimen_records.txt'
    termtokenfile = testdatapath + 'test_eight_specimen_records.csv'
    extractvaluesfile1 = testdatapath + 'test_eight_specimen_records.csv'

    csvcompositepath = testdatapath + 'test_csv*.csv'
    tsvcompositepath = testdatapath + 'test_tsv*.txt'
    mixedcompositepath = testdatapath + 'test_*_specimen_records.*'

    # following are files output during the tests, remove these in dispose()
    csvwriteheaderfile = testdatapath + 'test_write_header_file.csv'
    tsvfromcsvfile1 = testdatapath + 'test_tsv_from_csv_1.txt'
    tsvfromcsvfile2 = testdatapath + 'test_tsv_from_csv_2.txt'
    testvocabfile = testdatapath + 'test_vocab_file.csv'
    testtokenreportfile = testdatapath + 'test_token_report_file.txt'
    testencoding = testdatapath + 'test_encoding.txt'
    testnonprinting = testdatapath + 'test_nonprinting_out.txt'
    newlinecondenser = testdatapath + 'test_newlinecondenser_out.txt'

    def dispose(self):
        csvwriteheaderfile = self.csvwriteheaderfile
        tsvfromcsvfile1 = self.tsvfromcsvfile1
        tsvfromcsvfile2 = self.tsvfromcsvfile2
        testvocabfile = self.testvocabfile
        testtokenreportfile = self.testtokenreportfile
        testencoding = self.testencoding
        testnonprinting = self.testnonprinting
        newlinecondenser = self.newlinecondenser
        if os.path.isfile(csvwriteheaderfile):
            os.remove(csvwriteheaderfile)
        if os.path.isfile(tsvfromcsvfile1):
            os.remove(tsvfromcsvfile1)
        if os.path.isfile(tsvfromcsvfile2):
            os.remove(tsvfromcsvfile2)
        if os.path.isfile(testvocabfile):
            os.remove(testvocabfile)
        if os.path.isfile(testtokenreportfile):
            os.remove(testtokenreportfile)
        if os.path.isfile(testencoding):
            os.remove(testencoding)
        if os.path.isfile(testnonprinting):
            os.remove(testnonprinting)
        if os.path.isfile(newlinecondenser):
            os.remove(newlinecondenser)
        return True

class DWCAUtilsTestCase(unittest.TestCase):
    def setUp(self):
        self.framework = DWCAUtilsTestFramework()

    def tearDown(self):
        self.framework.dispose()
        self.framework = None

    def test_source_files_exist(self):
        print('Running test_source_files_exist')
        non_printing_file = self.framework.non_printing_file
        encodedfile_utf8 = self.framework.encodedfile_utf8
        encodedfile_latin_1 = self.framework.encodedfile_latin_1
        encodedfile_windows_1252 = self.framework.encodedfile_windows_1252
        csvreadheaderfile = self.framework.csvreadheaderfile
        csvreadheaderfile2 = self.framework.csvreadheaderfile2
        tsvreadheaderfile = self.framework.tsvreadheaderfile
        tsvtest1 = self.framework.tsvtest1
        tsvtest2 = self.framework.tsvtest2
        csvtest1 = self.framework.csvtest1
        csvtest2 = self.framework.csvtest2
        csvtotsvfile1 = self.framework.csvtotsvfile1
        csvtotsvfile2 = self.framework.csvtotsvfile2
        geogvocabfile = self.framework.geogvocabfile
        compositetestfile = self.framework.compositetestfile
        fieldcountestfile1 = self.framework.fieldcountestfile1
        fieldcountestfile2 = self.framework.fieldcountestfile2
        fieldcountestfile3 = self.framework.fieldcountestfile3
        monthvocabfile = self.framework.monthvocabfile
        termrowcountfile1 = self.framework.termrowcountfile1
        termrowcountfile2 = self.framework.termrowcountfile2
        termtokenfile = self.framework.termtokenfile

        file = non_printing_file
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = encodedfile_utf8
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = encodedfile_latin_1
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = encodedfile_windows_1252
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = csvreadheaderfile
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = csvreadheaderfile2
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = tsvreadheaderfile
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = tsvtest1
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = tsvtest2
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = csvtest1
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = csvtest2
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = csvtotsvfile1
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = csvtotsvfile2
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = monthvocabfile
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = geogvocabfile
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = compositetestfile
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = fieldcountestfile1
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = fieldcountestfile2
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = fieldcountestfile3
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = termrowcountfile1
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = termrowcountfile2
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

        file = termtokenfile
        s = 'File %s does not exist' % file
        self.assertTrue(os.path.isfile(file), s)

    def test_count_rows(self):
        print('Running test_count_rows')
        non_printing_file = self.framework.non_printing_file
        encodedfile_utf8 = self.framework.encodedfile_utf8
        geogvocabfile = self.framework.geogvocabfile

        file = non_printing_file
        count = count_rows(file)
        expected = 11
        s = 'Number of rows (%s) in %s not as expected (%s)' % (count, file, expected)
        self.assertEqual(count, expected, s)

        file = encodedfile_utf8
        count = count_rows(file)
        expected = 10
        s = 'Number of rows (%s) in %s not as expected (%s)' % (count, file, expected)
        self.assertEqual(count, expected, s)

        file = geogvocabfile
        count = count_rows(file)
        expected = 19
        s = 'Number of rows (%s) in %s not as expected (%s)' % (count, file, expected)
        self.assertEqual(count, expected, s)

    def test_tsv_dialect(self):
        print('Running test_tsv_dialect')
        dialect = tsv_dialect()
        self.assertEqual(dialect.delimiter, '\t',
            'incorrect delimiter for tsv')
        self.assertEqual(dialect.lineterminator, '\r',
            'incorrect lineterminator for tsv')
        self.assertEqual(dialect.escapechar, '',
            'incorrect escapechar for tsv')
        self.assertEqual(dialect.quotechar, '',
            'incorrect quotechar for tsv')
        self.assertTrue(dialect.doublequote,
            'doublequote not set to True for tsv')
        self.assertEqual(dialect.quoting, 3,
            'quoting not set to csv.QUOTE_NONE for tsv')
        self.assertTrue(dialect.skipinitialspace,
            'skipinitialspace not set to True for tsv')
        self.assertFalse(dialect.strict,
            'strict not set to False for tsv')

    def test_csv_file_dialect(self):
        print('Running test_csv_file_dialect')
        csvreadheaderfile = self.framework.csvreadheaderfile
        dialect = csv_file_dialect(csvreadheaderfile)
        self.assertIsNotNone(dialect, 'unable to detect csv file dialect')
        self.assertEqual(dialect.delimiter, ',',
            'incorrect delimiter detected for csv file')
        self.assertEqual(dialect.lineterminator, '\n',
            'incorrect lineterminator for csv file')
        self.assertEqual(dialect.escapechar, '\\',
            'incorrect escapechar for csv file')
        self.assertEqual(dialect.quotechar, '"',
            'incorrect quotechar for csv file')
        self.assertTrue(dialect.doublequote,
            'doublequote not set to True for csv file')
        self.assertEqual(dialect.quoting, csv.QUOTE_MINIMAL,
            'quoting not set to csv.QUOTE_MINIMAL for csv file')
        self.assertTrue(dialect.skipinitialspace,
            'skipinitialspace not set to True for csv file')
        self.assertFalse(dialect.strict,
            'strict not set to False for csv file')

    def test_csv_file_dialect_no_delimiter(self):
        print('Running test_csv_file_dialect_no_delimiter')
        csvreadheaderfile = self.framework.csvreadheaderfile2
        dialect = csv_file_dialect(csvreadheaderfile)
        self.assertIsNotNone(dialect, 'unable to detect csv file dialect')
        self.assertEqual(dialect.delimiter, ',',
            'incorrect delimiter detected for csv file')
        self.assertEqual(dialect.lineterminator, '\n',
            'incorrect lineterminator for csv file')
        self.assertEqual(dialect.escapechar, '\\',
            'incorrect escapechar for csv file')
        self.assertEqual(dialect.quotechar, '"',
            'incorrect quotechar for csv file')
        self.assertTrue(dialect.doublequote,
            'doublequote not set to True for csv file')
        self.assertEqual(dialect.quoting, csv.QUOTE_MINIMAL,
            'quoting not set to csv.QUOTE_MINIMAL for csv file')
        self.assertTrue(dialect.skipinitialspace,
            'skipinitialspace not set to True for csv file')
        self.assertFalse(dialect.strict,
            'strict not set to False for csv file')

    def test_tsv_file_dialect(self):
        print('Running test_tsv_file_dialect')
        tsvreadheaderfile = self.framework.tsvreadheaderfile
        dialect = csv_file_dialect(tsvreadheaderfile)
        self.assertIsNotNone(dialect, 'unable to detect tsv file dialect')
        self.assertEqual(dialect.delimiter, '\t',
            'incorrect delimiter detected for csv file')
        self.assertEqual(dialect.lineterminator, '\r',
            'incorrect lineterminator for csv file')
        self.assertEqual(dialect.escapechar, '',
            'incorrect escapechar for csv file')
        self.assertEqual(dialect.quotechar, '',
            'incorrect quotechar for csv file')
        self.assertTrue(dialect.doublequote,
            'doublequote not set to False for csv file')
        self.assertEqual(dialect.quoting, csv.QUOTE_NONE,
            'quoting not set to csv.QUOTE_NONE for csv file')
        self.assertTrue(dialect.skipinitialspace,
            'skipinitialspace not set to True for csv file')
        self.assertFalse(dialect.strict,
            'strict not set to False for csv file')

    def test_read_header1(self):
        print('Running test_read_header1')
        csvreadheaderfile = self.framework.csvreadheaderfile
        header = read_header(csvreadheaderfile)
        expected = ['catalogNumber ', 'recordedBy', 'fieldNumber ', 'year', 'month', 
        'day', 'decimalLatitude ', 'decimalLongitude ', 'geodeticDatum ', 'country', 
        'stateProvince', 'county', 'locality', 'family ', 'scientificName ', 
        'scientificNameAuthorship ', 'reproductiveCondition ', 'InstitutionCode ', 
        'CollectionCode ', 'DatasetName ', 'Id']
        self.assertEqual(len(header), 21, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_read_header2(self):
        print('Running test_read_header2')
        tsvheaderfile = self.framework.tsvtest1
        header = read_header(tsvheaderfile)
        expected = ['materialSampleID', 'principalInvestigator', 'locality', 'phylum', '']
        self.assertEqual(len(header), 5, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_read_header3(self):
        print('Running test_read_header3')
        csvheaderfile = self.framework.csvtest1
        header = read_header(csvheaderfile)
        expected = ['materialSampleID', 'principalInvestigator', 'locality', 'phylum', '']
        self.assertEqual(len(header), 5, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_read_header4(self):
        print('Running test_read_header4')
        tsvheaderfile = self.framework.tsvtest2
        header = read_header(tsvheaderfile)
        expected = ['materialSampleID', 'principalInvestigator', 'locality', 'phylum', 
        'decimalLatitude', 'decimalLongitude']
        self.assertEqual(len(header), 6, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_read_header5(self):
        print('Running test_read_header5')
        csvheaderfile = self.framework.csvtest2
        header = read_header(csvheaderfile)
        expected = ['materialSampleID', 'principalInvestigator', 'locality', 'phylum', 
        'decimalLatitude', 'decimalLongitude']
        self.assertEqual(len(header), 6, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_read_header6(self):
        print('Running test_read_header6')
        encodedfile_latin_1 = self.framework.encodedfile_latin_1
        header = read_header(encodedfile_latin_1)
        expected = ['id', 'class', 'coordinateUncertaintyInMeters', 'country',
            'eventDate', 'family', 'genus', 'decimalLatitude', 'decimalLongitude',
            'locality', 'scientificName', 'specificEpithet']
        self.assertEqual(len(header), 12, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_read_header7(self):
        print('Running test_read_header7')
        encodedfile_windows_1252 = self.framework.encodedfile_windows_1252
        header = read_header(encodedfile_windows_1252)
        expected = ['id','dwc:class','dwc:coordinateUncertaintyInMeters','dwc:country',
            'dwc:eventDate','dwc:family','dwc:genus','lat','lon','dwc:locality',
            'dwc:scientificName','dwc:specificEpithet']
        self.assertEqual(len(header), 12, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_composite_header(self):
        print('Running test_composite_header')
        csvcompositepath = self.framework.csvcompositepath
        tsvcompositepath = self.framework.tsvcompositepath
        mixedcompositepath = self.framework.mixedcompositepath
        header = composite_header(csvcompositepath)
        expected = ['decimalLatitude', 'decimalLongitude', 'locality', 
        'materialSampleID', 'phylum', 'principalInvestigator']
        self.assertEqual(len(header), 6, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

        header = composite_header(mixedcompositepath)
        expected = ['BCID', 'CollectionCode', 'DatasetName', 'Id', 'InstitutionCode',
        'associatedMedia', 'associatedReferences', 'associatedSequences', 
        'associatedTaxa', 'basisOfIdentification', 'catalogNumber', 'class', 
        'coordinateUncertaintyInMeters', 'country', 'county', 'day', 'dayCollected',
        'dayIdentified', 'decimalLatitude', 'decimalLongitude', 'establishmentMeans', 
        'eventRemarks', 'extractionID', 'family', 'fieldNotes','fieldNumber', 
        'fundingSource', 'geneticTissueType', 'genus', 'geodeticDatum', 
        'georeferenceProtocol', 'habitat', 'identifiedBy', 'island', 'islandGroup', 
        'length', 'lifeStage', 'locality', 'materialSampleID', 'maximumDepthInMeters', 
        'maximumDistanceAboveSurfaceInMeters', 'microHabitat', 'minimumDepthInMeters', 
        'minimumDistanceAboveSurfaceInMeters', 'month', 'monthCollected', 
        'monthIdentified', 'occurrenceID', 'occurrenceRemarks', 'order', 
        'permitInformation', 'phylum', 'plateID', 'preservative', 
        'previousIdentifications', 'previousTissueID', 'principalInvestigator', 
        'recordedBy', 'reproductiveCondition', 'sampleOwnerInstitutionCode', 
        'samplingProtocol', 'scientificName', 'scientificNameAuthorship', 'sex', 
        'species', 'stateProvince', 'subSpecies', 'substratum', 'taxonRemarks', 
        'tissueStorageID', 'vernacularName', 'weight', 'wellID', 'wormsID', 'year', 
        'yearCollected', 'yearIdentified']
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_write_header(self):
        print('Running test_write_header')
        csvreadheaderfile = self.framework.csvreadheaderfile
        csvwriteheaderfile = self.framework.csvwriteheaderfile
        header = read_header(csvreadheaderfile)
        dialect = tsv_dialect()
        self.assertIsNotNone(header, 'model header not found')

        written = write_header(csvwriteheaderfile, header, dialect)
        self.assertTrue(written, 'header not written to csvwriteheaderfile')

        writtenheader = read_header(csvwriteheaderfile)

        self.assertEqual(len(header), len(writtenheader),
            'incorrect number of fields in writtenheader')
        s = 'header:\n%s\nnot as written header:\n%s' % (header, writtenheader)
        self.assertEqual(header, writtenheader, s)

    def test_dialect_preservation(self):
        print('Running test_dialect_preservation')
        csvreadheaderfile = self.framework.csvreadheaderfile
        csvwriteheaderfile = self.framework.csvwriteheaderfile
        indialect = csv_file_dialect(csvreadheaderfile)
        self.assertIsNotNone(indialect, 'input dialect not determined')

        header = read_header(csvreadheaderfile, indialect)
        self.assertIsNotNone(header, 'header not read')

        written = write_header(csvwriteheaderfile, header, indialect)
        self.assertTrue(written, 'header not written')

        outdialect = csv_file_dialect(csvwriteheaderfile)
        self.assertIsNotNone(header, 'output dialect not determined')
        equaldialects = dialects_equal(indialect, outdialect)
        if equaldialects == False:
            print('input dialect:\n%s' % dialect_attributes(indialect))
            print('output dialect:\n%s' % dialect_attributes(outdialect))
        self.assertTrue(equaldialects, 'input and output dialects not the same')

    def test_convert_csv1(self):
        print('Running test_convert_csv1')
        csvfile = self.framework.csvtotsvfile1
        tsvfile = self.framework.tsvfromcsvfile1

        convert_csv(csvfile, tsvfile)
        written = os.path.isfile(tsvfile)
        self.assertTrue(written, 'tsv %s not written' % tsvfile)

        header = read_header(tsvfile)
        expected = ['materialSampleID', 'principalInvestigator', 'locality', 'phylum', '']
        self.assertEqual(len(header), 5, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_convert_csv2(self):
        print('Running test_convert_csv2')
        csvfile = self.framework.csvtotsvfile2
        tsvfile = self.framework.tsvfromcsvfile2

        convert_csv(csvfile, tsvfile)
        written = os.path.isfile(tsvfile)
        self.assertTrue(written, 'tsv %s not written' % tsvfile)

        header = read_header(tsvfile, tsv_dialect())
        expected = ['materialSampleID', 'principalInvestigator', 'locality', 'phylum', 
        'decimalLatitude', 'decimalLongitude']
        self.assertEqual(len(header), 6, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_convert_csv3(self):
        print('Running test_convert_csv3')
        csvfile = self.framework.encodedfile_latin_1
        tsvfile = self.framework.tsvfromcsvfile2

        convert_csv(csvfile, tsvfile)
        written = os.path.isfile(tsvfile)
        self.assertTrue(written, 'tsv %s not written' % tsvfile)

        header = read_header(tsvfile, tsv_dialect())
        expected = ['id','class','coordinateUncertaintyInMeters','country','eventDate',
            'family','genus','decimalLatitude','decimalLongitude','locality',
            'scientificName','specificEpithet']
        self.assertEqual(len(header), 12, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_convert_csv4(self):
        print('Running test_convert_csv4')
        csvfile = self.framework.symbiotafile
        tsvfile = self.framework.tsvfromcsvfile2

        convert_csv(csvfile, tsvfile)
        written = os.path.isfile(tsvfile)
        self.assertTrue(written, 'tsv %s not written' % tsvfile)

        header = read_header(tsvfile, tsv_dialect())
        expected = ['id', 'institutionCode', 'collectionCode', 'basisOfRecord', 
            'occurrenceID', 'catalogNumber', 'otherCatalogNumbers', 'kingdom', 'phylum',
            'class', 'order', 'family', 'scientificName', 'scientificNameAuthorship',
            'genus', 'specificEpithet', 'taxonRank', 'infraspecificEpithet',
            'identifiedBy', 'dateIdentified', 'identificationReferences', 
            'identificationRemarks', 'taxonRemarks', 'identificationQualifier',
            'typeStatus', 'recordedBy', 'recordedByID', 'associatedCollectors', 
            'recordNumber', 'eventDate', 'year', 'month', 'day', 'startDayOfYear',
            'endDayOfYear', 'verbatimEventDate', 'occurrenceRemarks', 'habitat', 
            'substrate', 'verbatimAttributes', 'fieldNumber', 'informationWithheld',
            'dataGeneralizations', 'dynamicProperties', 'associatedTaxa', 
            'reproductiveCondition', 'establishmentMeans', 'cultivationStatus', 
            'lifeStage', 'sex', 'individualCount', 'samplingProtocol', 'samplingEffort',
            'preparations', 'country', 'stateProvince', 'county', 'municipality', 
            'locality', 'locationRemarks', 'localitySecurity', 'localitySecurityReason',
            'decimalLatitude', 'decimalLongitude', 'geodeticDatum', 
            'coordinateUncertaintyInMeters', 'verbatimCoordinates', 'georeferencedBy',
            'georeferenceProtocol', 'georeferenceSources', 
            'georeferenceVerificationStatus', 'georeferenceRemarks', 
            'minimumElevationInMeters', 'maximumElevationInMeters', 
            'minimumDepthInMeters', 'maximumDepthInMeters', 'verbatimDepth', 
            'verbatimElevation', 'disposition', 'language', 'recordEnteredBy', 
            'modified', 'sourcePrimaryKey', 'collId', 'recordId', 'references']
        self.assertEqual(len(header), 86, 'incorrect number of fields in header')
        s = 'header:\n%s\nnot as expected:\n%s' % (header, expected)
        self.assertEqual(header, expected, s)

    def test_convert_csv5(self):
        print('Running test_convert_csv5')
        csvfile = self.framework.encodedfile_windows_1252
        tsvfile = self.framework.tsvfromcsvfile2

        try:
            convert_csv(csvfile, tsvfile)
            written = os.path.isfile(tsvfile)
            s = 'tsv %s written when convert should have raised exception' % tsvfile
            self.assertFalse(written, s)
        except:
            s = 'Exception (expected) converting file with UnicodeDecodeError'
            self.assertTrue(True)

    def test_split_path(self):
        print('Running test_split_path')
        path, fileext, filepattern = \
            split_path('../../data/tests/test_eight_specimen_records.csv')
        self.assertEqual(path, '../../data/tests', 'incorrect path')
        self.assertEqual(fileext, 'csv', 'incorrect file extension')
        self.assertEqual(filepattern, 'test_eight_specimen_records', 
            'incorrect file pattern')

    def test_header_map(self):
        print('Running test_header_map')
        header = ['b ', ' a', 'c	']
        result = header_map(header)
        expected = {'b':'b ', 'a':' a', 'c':'c	'}
        s = 'header map: %s not as \nexpected: %s' % (result, expected)
        self.assertEqual(result, expected, s)

        header = ['B ', ' A', ' c	']
        result = header_map(header)
        expected = {'b':'B ', 'a':' A', 'c':' c	'}
        s = 'header map: %s not as \nexpected: %s' % (result, expected)
        self.assertEqual(result, expected, s)

    def test_clean_header(self):
        print('Running test_clean_header')
        header = ['b ', ' a', 'c	']
        result = clean_header(header)
        expected = ['b', 'a', 'c']
        self.assertEqual(result, expected, 'short header failed to be cleaned properly')

        header = ['catalogNumber ','recordedBy','fieldNumber ','year','month','day', \
            'decimalLatitude ','decimalLongitude ','geodeticDatum ','country', \
            'stateProvince','county','locality','family ','scientificName ', \
            'scientificNameAuthorship ','reproductiveCondition ','InstitutionCode ', \
            'CollectionCode ','DatasetName ','Id']
        result = clean_header(header)
        expected = ['catalognumber','recordedby','fieldnumber','year','month','day', \
            'decimallatitude','decimallongitude','geodeticdatum','country', \
            'stateprovince','county','locality','family','scientificname', \
            'scientificnameauthorship','reproductivecondition','institutioncode', \
            'collectioncode','datasetname','id']
        self.assertEqual(result, expected, 'long header failed to be cleaned properly')

    def test_merge_headers(self):
        print('Running test_merge_headers')
        header1 = ['b', 'a', 'c']
        header2 = ['b', 'c ', 'd']
        header3 = ['e', 'd	', 'a']
        header4 = []
        header5 = ['']
        header6 = [' ']
        header7 = ['	']
        header8 = ['b', 'a', 'c', '  ']

        result = merge_headers(None)
        s = 'merging without header makes a header when it should not'
        self.assertIsNone(result, s)

        result = merge_headers(header4)
        s = 'merging an empty header makes a header when it should not'
        self.assertIsNone(result, s)

        result = merge_headers(header5)
        s = 'merging a header with only a blank field makes a header when it should not'
        self.assertIsNone(result, s)

        result = merge_headers(header6)
        s = 'merging header with only one field composed of a space character makes a '
        s += 'header when it should not'
        self.assertIsNone(result, s)

        result = merge_headers(header7)
        s = 'merging header with only one field composed of a tab character makes a '
        s += 'header when it should not'
        self.assertIsNone(result, s)

        result = merge_headers(None, header1)
        self.assertEqual(result, ['a', 'b', 'c'], 'merged new header did not sort')

        result = merge_headers(header1)
        self.assertEqual(result, ['a', 'b', 'c'], 'merged existing header did not sort')

        result = merge_headers(header1, header1)
        self.assertEqual(result, ['a', 'b', 'c'], 'redundant header merge failed')

        result = merge_headers(header1, header2)
        self.assertEqual(result, ['a', 'b', 'c', 'd'], 'bac-bcd header merge failed')

        result = merge_headers(header1, header2)
        result = merge_headers(result, header3)
        self.assertEqual(result, ['a', 'b', 'c', 'd', 'e'],
            'bac-bcd-eda header merge failed')

        result = merge_headers(header7, header8)
        self.assertEqual(result, ['a', 'b', 'c'],
            'headers with whitespace merge failed')

    def test_csv_field_checker(self):
        print('Running test_csv_field_checker')
        csvfile = self.framework.fieldcountestfile2
        result = csv_field_checker(csvfile)
        s = 'field checker found mismatched fields in %s when it should not' % csvfile
        self.assertIsNone(result, s)
        
        csvfile = self.framework.fieldcountestfile1
        result = csv_field_checker(csvfile)
        firstbadrow = result[0]
        s = 'field checker found first mismatched field in %s at row index %s'  \
            % (csvfile, firstbadrow)
        self.assertEqual(firstbadrow, 3, s)

        csvfile = self.framework.fieldcountestfile3
        result = csv_field_checker(csvfile)
        firstbadrow = result[0]
        s = 'field checker found first mismatched field in %s at row index %s'  \
            % (csvfile, firstbadrow)
        self.assertEqual(firstbadrow, 3, s)

        csvfile = self.framework.non_printing_file
        result = csv_field_checker(csvfile)
        firstbadrow = result[0]
        s = 'field checker found first mismatched field in %s at row index %s'  \
            % (csvfile, firstbadrow)
        self.assertEqual(firstbadrow, 4, s)

    def test_term_rowcount_from_file(self):
        print('Running test_term_rowcount_from_file')
        termrowcountfile = self.framework.termrowcountfile1
        rowcount = term_rowcount_from_file(termrowcountfile, 'country')
        expected = 8
        s = 'rowcount (%s) for country does not match expectation (%s)'  \
            % (rowcount, expected)
        self.assertEqual(rowcount, expected, s)

        termrowcountfile = self.framework.termrowcountfile2
        term = 'country'
        expected = 3
        rowcount = term_rowcount_from_file(termrowcountfile, term)
        s = 'rowcount (%s) for %s does not match expectation (%s) in %s'  \
            % (rowcount, term, expected, termrowcountfile)
        self.assertEqual(rowcount, expected, s)

        term = 'island'
        expected = 1
        rowcount = term_rowcount_from_file(termrowcountfile, term)
        s = 'rowcount (%s) for %s does not match expectation (%s) in %s'  \
            % (rowcount, term, expected, termrowcountfile)
        self.assertEqual(rowcount, expected, s)

    def test_represents_int(self):
        print('Running test_represents_int')
        result = represents_int(None)
        expected = False
        s = 'represents_int None result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int('A')
        expected = False
        s = 'represents_int \'A\' result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int("AAAA")
        expected = False
        s = 'represents_int "AAAA" result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int(self)
        expected = False
        s = 'represents_int self result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int(False)
        expected = True
        s = 'represents_int False result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int("0")
        expected = True
        s = 'represents_int "0" result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int(0)
        expected = True
        s = 'represents_int 0 result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int(-1)
        expected = True
        s = 'represents_int -1 result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int(1)
        expected = True
        s = 'represents_int 1 result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)

        result = represents_int(1.001)
        expected = True
        s = 'represents_int 1.001 result (%s) does not match expectation (%s)' % (result, expected)
        self.assertEqual(result, expected, s)
    
    def test_csv_file_encoding(self):
        print('Running test_csv_file_encoding')
        encodedfile_utf8 = self.framework.encodedfile_utf8
        encodedfile_latin_1 = self.framework.encodedfile_latin_1
        encodedfile_windows_1252 = self.framework.encodedfile_windows_1252

        encoding = csv_file_encoding(encodedfile_utf8)
        # UTF8 file containing only ascii and latin2 characters encoded as utf-8 is 
        # indistinguishable from latin1.
        expected = 'ISO-8859-1'
        s = 'file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)
        
        encoding = csv_file_encoding(encodedfile_latin_1)
        expected = 'ISO-8859-1'
        s = 'file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        encoding = csv_file_encoding(encodedfile_windows_1252)
        expected = 'Windows-1252'
        s = 'file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        # Passing in -1 should be an irrecoverable error
        encoding = csv_file_encoding(encodedfile_latin_1, -1)
        expected = 'ISO-8859-1'
        s = 'maxlines =-1 file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        # Passing in a string, 'A', should be an irrecoverable error
        encoding = csv_file_encoding(encodedfile_latin_1, 'A')
        expected = 'ISO-8859-1'
        s = 'maxlines = \'A\' file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        # Passing in a float, 1.001, is legal, but will read 2 lines
        encoding = csv_file_encoding(encodedfile_latin_1, 1.001)
        expected = 'ascii'
        s = 'maxlines = 1.001 file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        # If we read in only the first line there is not enough info for the detector
        # to make a fully informed decision, so it should return 'ascii'
        encoding = csv_file_encoding(encodedfile_latin_1, 1)
        expected = 'ascii'
        s = 'maxlines = 1 file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        encoding = csv_file_encoding(encodedfile_latin_1, 0)
        expected = 'ISO-8859-1'
        s = 'maxlines = 0 file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

        encoding = csv_file_encoding(encodedfile_latin_1, 1000)
        expected = 'ISO-8859-1'
        s = 'maxlines = 1000 file encoding (%s) does not match expectation (%s)' % (encoding, expected)
        self.assertEqual(encoding, expected, s)

    def test_purge_non_printing_from_file(self):
        print('Running test_purge_non_printing_from_file')
        testfile = self.framework.non_printing_file
        tempfile = self.framework.newlinecondenser

        success = purge_non_printing_from_file(testfile, tempfile)

        s = 'Unable to remove new lines from data content from %s' % testfile
        self.assertEqual(success, True, s)

        with open(tempfile, 'r', encoding='utf-8') as data:
            reader = csv.DictReader(data, dialect=csv_dialect())
            # header is the list as returned by the reader
            header=reader.fieldnames
            line = next(reader)
            expected = {'field1':'normal', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

            line = next(reader)
            expected = {'field1':'VT:-', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

            line = next(reader)
            expected = {'field1':'BS:-', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

            line = next(reader)
            expected = {'field1':'CR:-', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

            line = next(reader)
            expected = {'field1':'LF:-', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

            line = next(reader)
            expected = {'field1':'ESC:-', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

            line = next(reader)
            expected = {'field1':'NULL:-', 'field2':'end'}
            s = 'condensed line:\n%s\nfrom %s ' % (line, testfile)
            s += 'not as expected:\n%s' % expected
            self.assertEqual(line, expected, s)

    def test_fields_from_row(self):
        print('Running test_fields_from_row')        
        row = {
            'institutionCode':'MVZ',
            'collectionCode':'Mammals',
            'catalogNumber':'42',
            'country':'USA', 
            'stateProvince':'California', 
            'county':'Alameda Co'}

        fields = ['institutionCode', 'collectionCode', 'catalogNumber']
        found = extract_fields_from_row(row, fields)
        expected = {'institutionCode':'MVZ', 'collectionCode':'Mammals', 
            'catalogNumber':'42'}
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        fields = ['country', 'stateProvince', 'County']
        found = extract_fields_from_row(row, fields, '|')
        expected = {'country':'USA', 'stateProvince':'California', 'County':''}
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

    def test_extract_values_from_row(self):
        print('Running test_extract_values_from_row')                
        row = {
            'country|stateProvince|county':'USA|Montana|Missoula Co',
            'country':'USA', 
            'stateProvince':'Montana', 
            'county':'Missoula Co'}

        fields = ['country']
        found = extract_values_from_row(row, fields)
        expected = 'USA'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        fields = ['country', 'stateProvince']
        found = extract_values_from_row(row, fields, '|')
        expected = 'USA|Montana'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        found = extract_values_from_row(row, fields)
        expected = 'USAMontana'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        fields = ['country', 'stateProvince', 'county']
        found = extract_values_from_row(row, fields, '|')
        expected = 'USA|Montana|Missoula Co'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        found = extract_values_from_row(row, fields)
        expected = 'USAMontanaMissoula Co'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        fields = ['country|stateProvince|county']
        found = extract_values_from_row(row, fields)
        expected = 'USA|Montana|Missoula Co'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        fields = ['country|stateProvince|county']
        found = extract_values_from_row(row, fields, '|')
        expected = 'USA|Montana|Missoula Co'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        found = extract_values_from_row(row, fields)
        expected = 'USA|Montana|Missoula Co'
        s = 'Extracted values:\n%s not as expected:\n%s' % (found, expected)
        self.assertEqual(found, expected, s)

        fields = ['country|stateProvince']
        found = extract_values_from_row(row, fields)
        s = 'Extracted values:\n%s found where none expected' % found
        self.assertIsNone(found, s)

    def test_extract_values_from_file(self):
        print('Running test_extract_values_from_file')                
        extractvaluesfile1 = self.framework.extractvaluesfile1
        inputencoding = csv_file_encoding(extractvaluesfile1)
        fields = ['country']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        expected = ['United States']
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['stateProvince']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        expected = ['California', 'Colorado', 'Hawaii', 'Washington']
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['country', 'stateProvince', 'county']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            separator='|', encoding=inputencoding)
        expected = [
            'United States|California|', 
            'United States|California|Kern', 
            'United States|California|San Bernardino', 
            'United States|Colorado|', 
            'United States|Hawaii|Honolulu', 
            'United States|Washington|Chelan'
            ]
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        expected = [
            'United StatesCalifornia', 
            'United StatesCaliforniaKern', 
            'United StatesCaliforniaSan Bernardino', 
            'United StatesColorado', 
            'United StatesHawaiiHonolulu', 
            'United StatesWashingtonChelan'
            ]
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['CollectionCode ']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        expected = ['FilteredPush']
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['CollectionCode']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['collectionCode']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['collectioncode']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['collectioncode ']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding)
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['collectioncode ']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            separator='|', encoding=inputencoding)
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding, function=ustripstr)
        expected = ['FILTEREDPUSH']
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['country', 'stateProvince']
        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding, function=ustripstr, separator='|')
        expected = [
            'UNITED STATES|CALIFORNIA', 
            'UNITED STATES|COLORADO', 
            'UNITED STATES|HAWAII', 
            'UNITED STATES|WASHINGTON'
            ]
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        found = extract_values_from_file(extractvaluesfile1, fields, 
            encoding=inputencoding, function=ustripstr)
        expected = [
            'UNITED STATESCALIFORNIA', 
            'UNITED STATESCOLORADO', 
            'UNITED STATESHAWAII', 
            'UNITED STATESWASHINGTON'
            ]
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

    def test_extract_value_counts_from_file(self):
        print('Running test_extract_value_counts_from_file')                
        extractvaluesfile1 = self.framework.extractvaluesfile1

        fields = ['country']
        found = extract_value_counts_from_file(extractvaluesfile1, fields)
        expected = [('United States', 8)]
        s = 'Extracted values:\n%s' % found
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(found, expected, s)

        fields = ['stateProvince']
        found = extract_value_counts_from_file(extractvaluesfile1, fields)
        sortedlist = sorted(found)
        expected = [(u'California', 5), (u'Colorado', 1), (u'Hawaii', 1), (u'Washington', 1)]
        s = 'Extracted values:\n%s' % sortedlist
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(sortedlist, expected, s)        

        fields = ['country', 'stateProvince']
        found = extract_value_counts_from_file(extractvaluesfile1, fields, '|')
        sortedlist = sorted(found)
        expected = [
            ('United States|California', 5), 
            ('United States|Colorado', 1), 
            ('United States|Hawaii', 1), 
            ('United States|Washington', 1)]
        s = 'Extracted values:\n%s' % sortedlist
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(sortedlist, expected, s)

        found = extract_value_counts_from_file(extractvaluesfile1, fields)
        sortedlist = sorted(found)
        expected = [
            ('United StatesCalifornia', 5),  
            ('United StatesColorado', 1), 
            ('United StatesHawaii', 1),
            ('United StatesWashington', 1)]
        s = 'Extracted values:\n%s' % sortedlist
        s += ' not as expected:\n%s' % expected
        s += ' from %s' % extractvaluesfile1
        self.assertEqual(sortedlist, expected, s)

    def test_strip_list(self):
        print('Running test_strip_list')                
        inputlist = [' a ', 'b', ' c', 'd ', None]
        outputlist = strip_list(inputlist)
        expected = ['a', 'b', 'c', 'd', 'field_5']
        s = 'Strippped list:\n%s' % outputlist
        s += ' not as expected:\n%s' % expected
        self.assertEqual(outputlist, expected, s)

    def test_get_guid(self):
        print('Running test_get_guid')                
        guid = get_guid('uuid')
        expected = 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'
        s = 'GUID:\n%s' % guid
        s += ' not in expected format:\n%s' % expected
        guidlength = len(guid)
        self.assertEqual(guidlength, 36, s)

    def test_dwc_ordered_header(self):
        print('Running test_dwc_ordered_header')                
        header = ['genus', 'identifiedBy', 'recordedBy', 'country', 'occurrenceID', 
            'Weight', 'catalogNumber', 'eventDate', 'basisOfRecord', 'type']
        found = dwc_ordered_header(header)
        expected = ['type', 'basisOfRecord', 'occurrenceID', 'catalogNumber', 
            'recordedBy', 'eventDate', 'country', 'identifiedBy', 'genus', 'Weight']
        s = 'DwC-ordered header (%s) does not match expected (%s) ' % (found, expected)
        self.assertEqual(found, expected, s)

if __name__ == '__main__':
    print('=== dwca_utils_test.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
