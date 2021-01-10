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
__version__ = "dwca_vocab_utils_tests.py 2021-01-10T00:50-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/test/dwca_vocab_utils_test.py"

# This file contains unit tests for the functions in dwca_vocab_utils.
#
# Example:
#
# python dwca_vocab_utils_tests.py

from dwca_terms import controlledtermlist
from dwca_terms import geogkeytermlist
from dwca_terms import vocabfieldlist
from dwca_utils import csv_dialect
from dwca_utils import extract_values_from_file
from dwca_utils import read_header
from dwca_utils import tsv_dialect
from dwca_utils import ustripstr
from dwca_vocab_utils import compose_dict_from_key
from dwca_vocab_utils import compose_key_from_list
from dwca_vocab_utils import compose_key_from_row
from dwca_vocab_utils import darwin_cloud_vocab_dict_from_file
from dwca_vocab_utils import darwinize_list
from dwca_vocab_utils import darwinize_dict
from dwca_vocab_utils import distinct_vocabs_to_file
from dwca_vocab_utils import matching_vocab_dict_from_file
from dwca_vocab_utils import missing_vocab_list_from_file
from dwca_vocab_utils import not_in_list
from dwca_vocab_utils import term_values_recommended
from dwca_vocab_utils import terms_not_in_darwin_cloud
from dwca_vocab_utils import terms_not_in_dwc
from dwca_vocab_utils import vetted_vocab_dict_from_file
from dwca_vocab_utils import vocab_dialect
from dwca_vocab_utils import vocab_dict_from_file
from dwca_vocab_utils import vocabheader
from dwca_vocab_utils import writevocabheader
import os
import unittest
import csv

class DWCAVocabUtilsTestFramework():
    # testdatapath is the location of example files to test with
    testdatapath = '../data/tests/'
    # vocabpath is the location of vocabulary files to test with
    vocabpath = '../vocabularies/'

    # following are files used as input during the tests, don't remove these
    compositetestfile = testdatapath + 'test_eight_specimen_records.csv'
    monthvocabfile = vocabpath + 'month.txt'
    testmonthvocabfile = testdatapath + 'test_month.txt'
    geogvocabfile = vocabpath + 'dwc_geography.txt'
    darwincloudfile = vocabpath + 'darwin_cloud.txt'
    geogcountfileutf8 = testdatapath + "test_geography_count_utf8.csv"
    geogfileutf8 = testdatapath + "test_geography_utf8.csv"

    # following are files output during the tests, remove these in dispose()
    tsvfromcsvfile1 = testdatapath + 'test_tsv_from_csv_1.txt'
    tsvfromcsvfile2 = testdatapath + 'test_tsv_from_csv_2.txt'
    testvocabfile = testdatapath + 'test_vocab_file.csv'
    writevocabheadertestfile = testdatapath + 'test_write_vocabheader.txt'
    recommendedreporttestfile = testdatapath + 'test_term_recommended_report.txt'
    termcountreporttestfile = testdatapath + 'test_term_count_report.txt'
    counttestfile = testdatapath + 'test_three_specimen_records.txt'

    def dispose(self):
        tsvfromcsvfile1 = self.tsvfromcsvfile1
        tsvfromcsvfile2 = self.tsvfromcsvfile2
        testvocabfile = self.testvocabfile
        recommendedreporttestfile = self.recommendedreporttestfile
        termcountreporttestfile = self.termcountreporttestfile
        if os.path.isfile(tsvfromcsvfile1):
            os.remove(tsvfromcsvfile1)
        if os.path.isfile(tsvfromcsvfile2):
            os.remove(tsvfromcsvfile2)
        if os.path.isfile(recommendedreporttestfile):
            os.remove(recommendedreporttestfile)
        if os.path.isfile(termcountreporttestfile):
            os.remove(termcountreporttestfile)
        if os.path.isfile(testvocabfile):
            os.remove(testvocabfile)
        return True

class DWCAVocabUtilsTestCase(unittest.TestCase):
    def setUp(self):
        self.framework = DWCAVocabUtilsTestFramework()

    def tearDown(self):
        self.framework.dispose()
        self.framework = None

    def test_source_files_exist(self):
        print('Running test_source_files_exist')
        vocabpath = self.framework.vocabpath
        testdatapath = self.framework.testdatapath
        compositetestfile = self.framework.compositetestfile
        monthvocabfile = self.framework.monthvocabfile
        geogvocabfile = self.framework.geogvocabfile
        geogcountfileutf8 = self.framework.geogcountfileutf8
        geogfileutf8 = self.framework.geogfileutf8
        dialect = vocab_dialect()
        
        for field in controlledtermlist:
            vocabfile = vocabpath + field + '.txt'
            if not os.path.isfile(vocabfile):
                success = writevocabheader(vocabfile, vocabfieldlist, dialect)
            self.assertTrue(os.path.isfile(vocabfile), vocabfile + ' does not exist')

        self.assertTrue(os.path.isfile(geogvocabfile), geogvocabfile + ' does not exist')

        s = geogcountfileutf8 + ' does not exist'
        self.assertTrue(os.path.isfile(geogcountfileutf8), s)

        s = geogfileutf8 + ' does not exist'
        self.assertTrue(os.path.isfile(geogfileutf8), s)

    def test_vocab_headers_correct(self):
        print('Running test_vocab_headers_correct')
        vocabpath = self.framework.vocabpath
        vocabdialect = vocab_dialect()
        vocabencoding = 'utf-8'
        
        for field in controlledtermlist:
            vocabfile = vocabpath + field + '.txt'
            if not os.path.isfile(vocabfile):
                success = writevocabheader(vocabfile, vocabfieldlist, vocabdialect, 
                    vocabencoding)
            header = read_header(vocabfile, vocabdialect, vocabencoding)
            expected = [field.lower()] + vocabfieldlist
            s = 'File: %s\nheader: %s\n' % (vocabfile, header)
            s += 'not as expected: %s' % expected
            self.assertEqual(header, expected, s)

    def test_read_vocab_header(self):
        print('Running test_read_vocab_header')
        vocabdialect = vocab_dialect()
        vocabencoding = 'utf-8'
        
        monthvocabfile = self.framework.monthvocabfile
        header = read_header(monthvocabfile, vocabdialect, vocabencoding)
        found = len(header)
        expected = 3
        s = 'Found %s fields in header. Expected %s' % (found, expected)
        self.assertEqual(found, expected, s)

        expected = ['month'] + vocabfieldlist
        s = 'File: %s\nheader: %s\n' % (monthvocabfile, header)
        s += 'not as expected: %s' % expected
        self.assertEqual(header, expected, s)

    def test_vocab_dict_from_file(self):
        print('Running test_vocab_dict_from_file')
        monthvocabfile = self.framework.monthvocabfile
        testmonthvocabfile = self.framework.testmonthvocabfile
        geogcountfileutf8 = self.framework.geogcountfileutf8

        monthdict = vocab_dict_from_file(testmonthvocabfile, 'month')
        expected = 5
        s = 'Found %s items in month dictionary ' % len(monthdict)
        s += 'from %s, expected %s.' % (testmonthvocabfile, expected)
        self.assertEqual(len(monthdict), expected, s)

        monthdict = vocab_dict_from_file(monthvocabfile, 'month')

        seek = 'VI'
        s = "%s not found in month dictionary:\n%s" % (seek, monthdict)
        self.assertTrue('VI' in monthdict, s)

        field = 'vetted'
        expected = '1'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)
        field = 'standard'
        expected = '6'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)

        seek = '5'
        s = "%s not found in month dictionary:\n%s" % (seek, monthdict)
        self.assertTrue(seek in monthdict, s)

        field = 'vetted'
        expected = '1'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)

        field = 'standard'
        expected = '5'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)

        # Check that the entries in the dictionary are converted using the function
        monthdict = vocab_dict_from_file(testmonthvocabfile, 'month', function=ustripstr)
        # 'vi' is in the vocabfile, upstripstr should convert it to 'VI' in monthdict
        seek = 'VI'

        field = 'vetted'
        expected = '1'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)
        field = 'standard'
        expected = '6'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)

        # 'VII' is in the vocabfile, upstripstr should convert it to 'VII' in monthdict
        seek = 'VII'

        field = 'vetted'
        expected = '1'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)
        field = 'standard'
        expected = '7'
        found = monthdict[seek][field]
        s = "value of %s ('%s') not equal to '%s' " % (field, found, expected)
        s += "for vocab value %s" % seek
        self.assertEqual(found, expected, s)

    def test_missing_vocab_list_from_file(self):
        print('Running test_missing_vocab_list_from_file')
        geogcountfileutf8 = self.framework.geogcountfileutf8
        geogfileutf8 = self.framework.geogfileutf8

        v = u'North America|Canada||Alberta|||Brûlé Lake||'
        checklist = [v]
        missinglist = missing_vocab_list_from_file(checklist, geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect())
        s = "missinglist not empty for %s" % v
        self.assertEqual(len(missinglist), 0, s)

        vocabencoding = 'utf-8'
        # Test non-unicode string matching
        v = 'North America|Canada||Alberta|||Brûlé Lake||'
        checklist = [v]
        vocablist = vocab_dict_from_file(geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect(), encoding=vocabencoding)
        missinglist = missing_vocab_list_from_file(checklist, geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect(), encoding=vocabencoding)
        #print('checklist: %s' % checklist)
        #print('missinglist: %s' % missinglist)
        #print('vocablist: %s' % vocablist)
        s = "missinglist empty for %s" % v
        self.assertEqual(len(missinglist), 0, s)

        # Test unicode string matching
        v = u'North America|Canada||Alberta|||Brûlé Lake||'
        checklist = [v]
        missinglist = missing_vocab_list_from_file(checklist, geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect(), encoding=vocabencoding)
        s = "missinglist not empty for %s" % v
        self.assertEqual(len(missinglist), 0, s)

        v = 'North America|Canada||Alberta|||Brule Lake||'
        checklist = [v]
        missinglist = missing_vocab_list_from_file(checklist, geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect(), encoding=vocabencoding)
        s = "missinglist empty for %s" % v
        self.assertEqual(len(missinglist), 1, s)

    def test_matching_vocab_dict_from_file(self):
        print('Running test_matching_vocab_dict_from_file')
        monthvocabfile = self.framework.monthvocabfile
        geogcountfileutf8 = self.framework.geogcountfileutf8

        checklist = ['VI', '5', 'fdsf']
        matchingdict = matching_vocab_dict_from_file(checklist, monthvocabfile, 'month', '|')
        s = 'month vocab at %s does has %s matching items in it instead of 2' % \
            (monthvocabfile, len(matchingdict))
        self.assertEqual(len(matchingdict), 2, s)

        matchingdict = matching_vocab_dict_from_file(checklist, monthvocabfile, 'month')
        s = 'month vocab at %s does has %s matching items in it instead of 2' % \
            (monthvocabfile, len(matchingdict))
        self.assertEqual(len(matchingdict), 2, s)

        self.assertTrue('VI' in matchingdict,"'VI' not found in month dictionary")
        self.assertEqual(matchingdict['VI']['vetted'], '1', 
            "value of 'vetted' not equal to 1 for vocab value 'VI'")
        self.assertEqual(matchingdict['VI']['standard'], '6', 
            "value of 'standard' not equal to '6' for vocab value 'VI'")

        self.assertTrue('5' in matchingdict,"'5' not found in month dictionary")
        self.assertEqual(matchingdict['5']['vetted'], '1', 
            "value of 'vetted' not equal to 1 for vocab value '5'")
        self.assertEqual(matchingdict['5']['standard'], '5', 
            "value of 'standard' not equal to '5' for vocab value '5'")

        v = u'North America|Canada||Alberta|||Brûlé Lake||'
        checklist = [v]
        matchingdict = matching_vocab_dict_from_file(checklist, geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect())
        s = "matchingdict empty for %s" % v
        self.assertEqual(len(matchingdict), 1, s)

        v = 'North America|Canada||Alberta|||Brule Lake||'
        checklist = [v]
        matchingdict = matching_vocab_dict_from_file(checklist, geogcountfileutf8, 
            'continent|country|countrycode|stateprovince|county|municipality|waterbody|islandgroup|island',
            dialect=csv_dialect())
        s = "matchingdict empty for %s" % v
        self.assertEqual(len(matchingdict), 0, s)

    def test_vetted_vocab_dict_from_file(self):
        print('Running test_vetted_vocab_dict_from_file')
        monthvocabfile = self.framework.monthvocabfile
        testmonthvocabfile = self.framework.testmonthvocabfile

        monthdict = vetted_vocab_dict_from_file(testmonthvocabfile, 'month')
        expected = 4
        s = 'Found %s items in month dictionary ' % len(monthdict)
        s += 'from %s, expected %s.' % (testmonthvocabfile, expected)
        self.assertEqual(len(monthdict), expected, s)

    def test_terms_not_in_dwc(self):
        print('Running test_terms_not_in_dwc')
        checklist = ['eventDate', 'verbatimEventDate', 'year', 'month', 'day', 
        'earliestDateCollected', '', 'latestDateCollected', 'YEAR', 'Year']
        notdwc = terms_not_in_dwc(checklist, casesensitive=True)
        expectedlist = ['YEAR', 'Year', 'earliestDateCollected', 'latestDateCollected']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        checklist = ['eventDate', 'verbatimEventDate', 'year', 'month', 'day', 
        'earliestDateCollected', '', 'latestDateCollected', 'YEAR', 'Year']
        notdwc = terms_not_in_dwc(checklist)
        expectedlist = ['EARLIESTDATECOLLECTED', 'LATESTDATECOLLECTED']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        checklist = ['catalogNumber','catalognumber', 'JUNK']
        notdwc = terms_not_in_dwc(checklist, casesensitive=True)
        expectedlist = ['JUNK', 'catalognumber']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        notdwc = terms_not_in_dwc(checklist, casesensitive=False)
        expectedlist = ['JUNK']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        notdwc = terms_not_in_dwc(checklist)
        expectedlist = ['JUNK']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

    def test_terms_not_in_darwin_cloud(self):
        print('Running test_terms_not_in_darwin_cloud')
        checklist = ['stuff', 'nonsense', 'Year']
        darwincloudfile = self.framework.darwincloudfile
        notdwc = terms_not_in_darwin_cloud(checklist, darwincloudfile)
        expectedlist = ['NONSENSE', 'STUFF']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        notdwc = terms_not_in_darwin_cloud(checklist, darwincloudfile, vetted=True, 
            casesensitive=True)
        expectedlist = ['Year', 'nonsense', 'stuff']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        notdwc = terms_not_in_darwin_cloud(checklist, darwincloudfile, vetted=True, 
            casesensitive=False)
        expectedlist = ['NONSENSE', 'STUFF']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

    def test_darwinize_list(self):
        print('Running test_darwinize_list')
        checklist = ['STUFF', 'Nonsense', 'Year', '  ', 'dwc:day', 'MONTH ', \
            'lifestage', 'Id']
        darwincloudfile = self.framework.darwincloudfile
        notdwc = darwinize_list(checklist, darwincloudfile)
        expectedlist = ['STUFF', 'Nonsense', 'year', 'UNNAMED_COLUMN_1', 'day', 'month', \
            'lifeStage', 'Id']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        checklist = ['InstitutionCode ', 'collectioncode', 'DATASETNAME']
        darwincloudfile = self.framework.darwincloudfile
        notdwc = darwinize_list(checklist, darwincloudfile)
        expectedlist = ['institutionCode', 'collectionCode', 'datasetName']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        checklist = [u'catalogNumber ', u'InstitutionCode ', u'CollectionCode ', u'Id']
        darwincloudfile = self.framework.darwincloudfile
        notdwc = darwinize_list(checklist, darwincloudfile)
        expectedlist = ['catalogNumber', 'institutionCode', 'collectionCode', 'Id']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

        checklist = [u'catalogNumber ', u'InstitutionCode ', u'CollectionCode ', u'Id']
        darwincloudfile = self.framework.darwincloudfile
        notdwc = darwinize_list(checklist, darwincloudfile, namespace='yes')
        expectedlist = ['dwc:catalogNumber', 'dwc:institutionCode', 'dwc:collectionCode', 
            'Id']
        s = 'Found:\n%s\nNot as expected:\n%s' % (notdwc, expectedlist)
        self.assertEqual(notdwc, expectedlist, s)

    def test_darwinize_dict(self):
        print('Running test_darwinize_dict')
        checkdict = {u'v_catalogNumber ':'a101', u'dwc:InstitutionCode ':'MACN', 
            u'CollectionCode ':'Mamm', u'ID':'someid'}
        darwincloudfile = self.framework.darwincloudfile
        result = darwinize_dict(checkdict, darwincloudfile, namespace=True)
        expecteddict = {'dwc:catalogNumber':'a101', 'dwc:institutionCode':'MACN', 
            'dwc:collectionCode':'Mamm', 'ID':'someid'}
        s = 'Found:\n%s\nNot as expected:\n%s' % (result, expecteddict)
        self.assertEqual(result, expecteddict, s)

        checkdict = {'STUFF':'a', 'Nonsense':'b', 'Year':2021, '  ':4, 'dwc:day':8, 
            'MONTH ':1, 'lifestage':'juvenile', 'Id':'ID', 'v_continent':'South America'}
        darwincloudfile = self.framework.darwincloudfile
        result = darwinize_dict(checkdict, darwincloudfile)
        expecteddict = {'STUFF':'a', 'Nonsense':'b', 'year':2021, 'UNNAMED_COLUMN_1':4, 
            'day':8, 'month':1, 'lifeStage':'juvenile', 'ID':'ID', 
            'continent':'South America'}
        s = 'Found:\n%s\nNot as expected:\n%s' % (result, expecteddict)
        self.assertEqual(result, expecteddict, s)

    def test_not_in_list(self):
        print('Running test_not_in_list')
        targetlist = ['b', 'a', 'c']
        checklist = ['c', 'd', 'a', 'e']
        newlist = not_in_list(targetlist, checklist)
        self.assertEqual(newlist, ['d', 'e'],
            'new values de for target list do not meet expectation')
        newlist = not_in_list(None, checklist)
        self.assertEqual(newlist, ['a', 'c', 'd', 'e'],
            'new values acde for targetlist do not meet expectation')

    def test_distinct_vocabs_to_file(self):
        print('Running test_distinct_vocabs_to_file')
        testvocabfile = self.framework.testvocabfile
        vocabencoding = 'utf-8'

        valuelist = ['b', 'a', 'c']
        writtenlist = distinct_vocabs_to_file(testvocabfile, valuelist, 'verbatim',
            dialect=tsv_dialect())
        expected = ['a', 'b', 'c']

        # Check that the testvocabfile exists
        s = 'writtenlist: %s not as expected: %s' % (writtenlist, expected)
        self.assertEqual(writtenlist, expected, s)
        check = os.path.isfile(testvocabfile)
        s = 'testvocabfile not written to %s for first checklist' % testvocabfile
        self.assertTrue(check, s)

        fulllist = extract_values_from_file(testvocabfile, ['verbatim'], encoding=vocabencoding)
        checklist = ['c', 'd', 'a', 'e']
        writtenlist = distinct_vocabs_to_file(testvocabfile, checklist, 'verbatim')
        expected = ['d', 'e']
        s = 'writtenlist: %s not as expected: %s' % (writtenlist, expected)
        self.assertEqual(writtenlist, expected, s)
        check = os.path.isfile(testvocabfile)
        s = 'testvocabfile not written to %s for second checklist' % testvocabfile
        self.assertTrue(check, s)

        fulllist = extract_values_from_file(testvocabfile, ['verbatim'], separator=None,
            dialect=None, encoding=vocabencoding)
        expected = ['a', 'b', 'c', 'd', 'e']
        s = 'Extracted values: %s\n not as expected: %s' % (fulllist, expected)
        self.assertEqual(fulllist, expected, s)

    def test_compose_key_from_list(self):
        print('Running test_compose_key_from_list')
        valuelist = ['a', 'b', 'c']
        key = compose_key_from_list(valuelist, '|')
        expected = 'a|b|c'
        self.assertEqual(key, expected, 
            'key value' + key + 'not as expected: ' + expected)

        key = compose_key_from_list(valuelist)
        expected = 'abc'
        self.assertEqual(key, expected, 
            'key value' + key + 'not as expected: ' + expected)

        key = compose_key_from_list(geogkeytermlist, '|')
        expected = 'continent|country|countryCode|stateProvince|county|municipality|waterBody|islandGroup|island'
        self.assertEqual(key, expected, 
            'geog key value:\n' + key + '\nnot as expected:\n' + expected)

        key = compose_key_from_list(geogkeytermlist)
        expected = 'continentcountrycountryCodestateProvincecountymunicipalitywaterBodyislandGroupisland'
        self.assertEqual(key, expected, 
            'geog key value:\n' + key + '\nnot as expected:\n' + expected)

    def test_vocabheader(self):
        print('Running test_vocabheader')
        # Example:
        # if keyfields = 'country|stateprovince|county'
        # and
        # vocabfieldlist = ['standard','vetted']
        # then the header will end up as 
        # 'country|stateprovince|county, country, stateprovince, county, standard, vetted'
        keyfields = 'country|stateprovince|county'
        header = vocabheader(keyfields, '|')
        expected = [keyfields] + ['country', 'stateprovince', 'county'] + vocabfieldlist
        s = 'header:\n%s\nnot as expected:\n%s' % (header,expected)
        self.assertEqual(header, expected, s)

        header = vocabheader(keyfields)
        expected = [keyfields] + vocabfieldlist
        s = 'header:\n%s\nnot as expected:\n%s' % (header,expected)
        self.assertEqual(header, expected, s)

    def test_writevocabheader(self):
        print('Running test_writevocabheader')
        writevocabheadertestfile = self.framework.writevocabheadertestfile
        fieldnames = ['country|stateprovince|county', 'standard', 'vetted', 'error', 
            'misplaced', 'unresolved', 'source', 'comment']
        dialect = vocab_dialect()
        success = writevocabheader(writevocabheadertestfile, fieldnames, dialect)
        self.assertTrue(success,'vocab header not written')
        
        header = read_header(writevocabheadertestfile)
        expected = ['country|stateprovince|county', 'standard', 'vetted', 'error', 
            'misplaced', 'unresolved', 'source', 'comment']
        s = 'header:\n%s\nfrom file: %s\nnot as expected:\n%s' \
            % (header,writevocabheadertestfile,expected)
        self.assertEqual(header, expected, s)

    def test_term_values_recommended(self):
        print('Running test_term_values_recommended')
        monthvocabfile = self.framework.monthvocabfile
        monthdict = { 
            'V': {'vetted':'1', 'standard':'5'}, 
            'junk': {'vetted':'0', 'standard':None} 
            }
        recommended = term_values_recommended(monthdict)
        expected = { 'V': {'vetted': '1', 'standard': '5'} }
        s = 'added_values:\n%s\nnot as expected:\n%s' \
            % (recommended, expected)
        self.assertEqual(recommended, expected, s)

    def test_compose_dict_from_key(self):
        print('Running test_compose_dict_from_key')
        key = '|United States|US'
        termlist = ['continent', 'country', 'countryCode']

        d = compose_dict_from_key('', termlist)
        s = 'compose_dict_from_key returned dict for empty input key'
        self.assertIsNone(d, s)

        d = compose_dict_from_key(key, None)
        s = 'compose_dict_from_key returned dict for empty termlist'
        self.assertIsNone(d, s)

        d = compose_dict_from_key(key, termlist, '|')
        try:
            continent = d['continent']
        except:
            continent = None
        s = 'continent not in composed dictionary'
        self.assertIsNotNone(continent, s)
        s = 'continent %s not zero length' % continent
        self.assertEqual(len(continent), 0, s)

        try:
            country = d['country']
        except:
            country = None
        s = 'country not in composed dictionary'
        self.assertIsNotNone(country, s)
        expected = 'United States'
        s = 'country %s not as expected %s' % (country, expected)
        self.assertEqual(country, expected, s)

        try:
            countrycode = d['countryCode']
        except:
            countrycode = None
        s = 'countrycode not in composed dictionary'
        self.assertIsNotNone(countrycode, s)
        expected = 'US'
        s = 'countryCode %s not as expected %s' % (countrycode, expected)
        self.assertEqual(countrycode, expected, s)

    def test_compose_key_from_row(self):
        print('Running test_compose_key_from_row')
        row = {'country':'United States', 'countryCode':'US'}
        terms = 'continent|country|countryCode'
        
        k = compose_key_from_row(None, terms)
        s = 'compose_key_from_row returned a key without an input row'
        self.assertIsNone(k, s)

        k = compose_key_from_row(row, '')
        s = 'compose_key_from_row returned a key without an input field list string'
        self.assertIsNone(k, s)

        k = compose_key_from_row(row, terms, separator=	'|')
        expected = '|United States|US'
        s = 'key %s not as expected %s' % (k, expected)
        self.assertEqual(k, expected, s)

        k = compose_key_from_row(row, terms)
        expected = ''
        s = 'key %s not as expected %s' % (k, expected)
        self.assertEqual(k, expected, s)

    def test_darwin_cloud_vocab_dict_from_file(self):
        print('Running test_darwin_cloud_vocab_dict_from_file')
        darwincloudfile = self.framework.darwincloudfile
        
        clouddict = darwin_cloud_vocab_dict_from_file(darwincloudfile)
        s = 'No Darwin Cloud dictionary loaded from %s' % darwincloudfile
        self.assertIsNotNone(clouddict, s)

        term = 'LAT'
        try:
            entry = clouddict[term]
        except:
            entry = None
        s = 'No Darwin Cloud dictionary entry for %s ' % term
        s += 'found in %s' % darwincloudfile
        self.assertIsNotNone(entry, s)

        seek = entry['namespace']
        expected = 'dwc'
        s = 'Namespace (%s) does not match expected (%s) ' % (seek, expected)
        s += 'in entry %s' % entry
        self.assertEqual(seek, expected, s)

if __name__ == '__main__':
    print('=== dwca_vocab_utils_test.py ===')
    #setup_actor_logging({'loglevel':'DEBUG'})
    unittest.main()
