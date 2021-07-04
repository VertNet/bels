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
__version__ = "dwca_vocab_utils.py 2021-06-30T16:53-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/dwca_vocab_utils.py"

# This file contains common utility functions for dealing with the vocabulary management
# for Darwin Core-related terms

from dwca_terms import simpledwctermlist
from dwca_terms import vocabfieldlist
from dwca_terms import vocabrowdict
from dwca_utils import csv_file_dialect
from dwca_utils import csv_file_encoding
from dwca_utils import extract_values_from_file
from dwca_utils import read_csv_row
from dwca_utils import read_header
from dwca_utils import tsv_dialect
from dwca_utils import ustripstr
from dwca_utils import write_header
import os.path
import logging
import copy
import csv

def vocabheader(key, separator=None):
    ''' Construct the header row for a vocabulary file. Begin with a field name equal to 
    the key variable, then add fields for the components of the key if it is composite 
    (i.e., it has field names separated by the separato, then add the remaining field 
    names from the standard vocabfieldlist.
    parameters:
        key - the field or separator-separated fieldnames that hold the distinct values 
            in the vocabulary file (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
    returns:
        fieldnames - list of fields in the vocabulary header
    Example:
      if key = 'country|stateprovince' 
      and separator =  '|'
      and vocabfieldlist = ['standard', 'vetted']
      then the header will end up as 
      ['country|stateprovince','country','stateprovince','standard','vetted']
    '''
    functionname = 'vocabheader()'

    if key is None:
        return None
    if separator is not None:
        composite = key.split(separator)
        if len(composite) > 1:
            return [key] + composite + vocabfieldlist
    return [key] + vocabfieldlist

def writevocabheader(fullpath, fieldnames, dialect=None):
    ''' Write a vocabulary header to a file in utf-8 using the chosen dialect.
    parameters:
        fullpath - the full path to the file to write into (required)
        fieldnames - list of field names in the header (required)
        dialect - csv.dialect object with the attributes of the vocabulary lookup file 
            (default None)
    returns:
        success - True if the header was written to the file, otherwise False
    '''
    functionname = 'writevocabheader()'

    if fullpath is None or len(fullpath) == 0:
        s = 'No vocabulary file given in %s.' % functionname
        logging.debug(s)
        return False

    if fieldnames is None or len(fieldnames) == 0:
        s = 'No list of field names given in %s.' % functionname
        logging.debug(s)
        return False

    if dialect is None:
        dialect = tsv_dialect()

    success = write_header(fullpath, fieldnames, dialect)
    if success == False:
        s = 'No header written to file %s in %s.' % (fullpath, functionname)
        logging.debug(s)
        return False

    s = 'Header written to %s in %s.' % (fullpath, functionname)
    logging.debug(s)
    return True

def compose_key_from_list(alist, separator=None):
    ''' Get a string consisting of the values in a list, separated by separator.
    parameters:
        alist - list of values to compose into a string. The values cannot contain 
            the separator string (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
    returns:
        key - composed string with values separated by separator
    '''
    functionname = 'compose_key_from_list()'

    if alist is None or len(alist)==0:
        s = 'No list given in %s.' % functionname
        logging.debug(s)
        return None

    n=0
    if separator is None:
        separator = ''
    for value in alist:
        if n==0:
            if value is None:
                return None
            key=value.strip()
            n=1
        else:
            if value is None:
                value=''
            key=key+separator+value.strip()
    return key

def vocab_dialect():
    ''' Get a dialect object with properties for vocabulary management files.
    parameters:
        None
    returns:
        dialect - a csv.dialect object with TSV attributes
    '''
    return tsv_dialect()

def matching_vocab_dict_from_file(
    checklist, vocabfile, key, separator=None, dialect=None, encoding=None):
    ''' Given a checklist of values, get matching values from a vocabulary file. Values
       can match exactly, or they can match after making them upper case and stripping 
       whitespace.
    parameters:
        checklist - list of values to get from the vocabfile (required)
        vocabfile - full path to the vocabulary lookup file (required)
        key - the field or separator-separated fieldnames that hold the distinct values 
            in the vocabulary file (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
        dialect - csv.dialect object with the attributes of the vocabulary lookup file 
            (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        matchingvocabdict - dictionary of complete vocabulary records matching the values 
            in the checklist
    '''
    functionname = 'matching_vocab_dict_from_file()'

    if checklist is None or len(checklist)==0:
        s = 'No list of values given in %s.' % functionname
        logging.debug(s)
        return None

    #print('checklist: %s' % checklist)

    vocabdict = vocab_dict_from_file(vocabfile, key, separator, dialect, encoding)
    if vocabdict is None or len(vocabdict)==0:
        s = 'No vocabdict constructed in %s' % functionname
        logging.debug(s)
        return None

    #print('vocabdict: %s' % vocabdict)

    matchingvocabdict = {}

    # Look through every value in the checklist
    for value in checklist:
        if separator is None:
            terms = [value]
        else:
            try:
                terms = value.split(separator)
            except Exception as e:
                s = 'Exception splitting value: %s Exception: %s ' % (value, e)
                s += 'in %s' % functionname
                logging.debug(s)
                terms = [value] # cop out
        newvalue = ''
        n=0
        for term in terms:
            if n==0:
                newvalue = ustripstr(term)
                n=1
            else:
                newvalue = newvalue + separator + ustripstr(term)

        # If the simplified version of the value is in the dictionary, get the 
        # vocabulary entry for it.
        if value in vocabdict or newvalue in vocabdict:
            matchingvocabdict[value]=vocabdict[newvalue]

    return matchingvocabdict

def missing_vocab_list_from_file(checklist, vocabfile, key, separator=None, dialect=None,
    encoding=None):
    ''' Given a checklist of values, get values not found in the given vocabulary file. 
       Values can match exactly, or they can match after making them upper case and 
       stripping whitespace.
    parameters:
        checklist - list of values to get from the vocabfile (required)
        vocabfile - full path to the vocabulary lookup file (required)
        key - the field or separator-separated fieldnames that hold the distinct values 
              in the vocabulary file (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
        dialect - csv.dialect object with the attributes of the vocabulary lookup file 
            (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        missingvocabdict - values in the checklist not found in the vocabulary file
    '''
    functionname = 'missing_vocab_list_from_file()'

    if checklist is None or len(checklist)==0:
        s = 'No list of values given in %s.' % functionname
        logging.debug(s)
        return None

    vocabdict = vocab_dict_from_file(vocabfile, key, separator, dialect, encoding)
    if vocabdict is None or len(vocabdict)==0:
        s = 'No vocabdict constructed in %s.' % functionname
        logging.debug(s)
        return None

    missingvocabset = set()

    # Look through every value in the checklist
    for value in checklist:
        if separator is None:
            terms = [value]
        else:
            try:
                terms = value.split(separator)
            except Exception as e:
                s = 'Exception splitting value: %s Exception: %s ' % (value, e)
                s += 'in %s' % functionname
                logging.debug(s)
                terms = [value] # cop out
        newvalue = ''
        n=0
        for term in terms:
            if n==0:
                newvalue = ustripstr(term)
                n=1
            else:
                newvalue = newvalue + separator + ustripstr(term)
        # If value or newvalue is in the vocabulary, nevermind
        if value in vocabdict or newvalue in vocabdict:
            pass
        # Otherwise, add the upper case, stripped value to the list
        else:
            missingvocabset.add(newvalue)

    return sorted(list(missingvocabset))

def vetted_vocab_dict_from_file(vocabfile, key, separator=None, dialect=None, 
    encoding=None):
    ''' Get the vetted vocabulary as a dictionary from a file.
    parameters:
        vocabfile - path to the vocabulary file (required)
        key - the field or separator-separated fieldnames that hold the distinct values 
            in the vocabulary file (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
        dialect - csv.dialect object with the attributes of the vocabulary lookup file
            (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        vocabdict - dictionary of complete vetted vocabulary records
    '''
    functionname = 'vetted_vocab_dict_from_file()'

    # No need to check for vocabfile, vocab_dict_from_file does that.
    thedict = vocab_dict_from_file(vocabfile, key, separator, dialect, encoding)
    vetteddict = {}
    for entry in thedict:
        if thedict[entry]['vetted'] == '1':
            vetteddict[entry]=thedict[entry]
    return vetteddict

def vocab_dict_from_file(
    vocabfile, key, separator=None, dialect=None, encoding=None, 
    function=None, *args, **kwargs):
    ''' Get a vocabulary as a dictionary from a file.
    parameters:
        vocabfile - path to the vocabulary file (required)
        key - the field or separator-separated fieldnames that hold the distinct values 
            in the vocabulary file (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
        dialect - a csv.dialect object with the attributes of the vocabulary file
            (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        function - function to call for each value to compare (default None)
        args - unnamed parameters to function as tuple (optional)
        kwargs - named parameters to function as dictionary (optional)
    Example:
       vocab_dict_from_file(v,k,function=ustripstr) would return all of the stripped, 
       uppercased keys and their values from the vocabfile v.
    returns:
        vocabdict - dictionary of complete vocabulary records
    '''
    functionname = 'vocab_dict_from_file()'

    if key is None or len(key.strip()) == 0:
        s = 'No key given in %s.' % functionname
        logging.debug(s)
        return None

    if vocabfile is None or len(vocabfile) == 0:
        s = 'No vocabulary file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(vocabfile) == False:
        s = 'Vocabulary file %s not found in %s.' % (vocabfile, functionname)
        logging.debug(s)
        return None

    if dialect is None:
        dialect = vocab_dialect()
    
    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(vocabfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Set up the field names to match the standard vocabulary header
    fieldnames = vocabheader(key, separator)

    # Create a dictionary to hold the vocabulary
    vocabdict = {}

    # Iterate through all rows in the input file
    for row in read_csv_row(vocabfile, dialect, encoding, header=True, 
            fieldnames=fieldnames):
        # Make a complete copy of the row
        rowdict = copy.deepcopy(row)
        value = row[key]
        # Remove the key from the row copy
        rowdict.pop(key)
        newvalue = value
        # If we are supposed to apply a function to the key value
        if function is not None:
            newvalue = function(value, *args, **kwargs)
        vocabdict[newvalue]=rowdict
                
    return vocabdict

def darwin_cloud_vocab_dict_from_file(vocabfile):
    ''' Get a Darwin Cloud vocabulary as a dictionary from a file.
    parameters:
        vocabfile - path to the vocabulary file (required)
    returns:
        vocabdict - dictionary of complete vocabulary records
    '''
    functionname = 'darwin_cloud_vocab_dict_from_file()'

    if vocabfile is None or len(vocabfile) == 0:
        s = 'No vocabulary file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(vocabfile) == False:
        s = 'Vocabulary file %s not found in %s.' % (vocabfile, functionname)
        logging.debug(s)
        return None

    dialect = csv_file_dialect(vocabfile)

    # Create a dictionary to hold the vocabulary
    vocabdict = {}

    header = read_header(vocabfile, dialect=dialect, encoding='utf8')

    # Iterate through all rows in the input file. Let read_csv_row figure out the dialect
    for row in read_csv_row(vocabfile, dialect=dialect, encoding='utf-8', header=True, 
            fieldnames=header):
        # Make a complete copy of the row
        rowdict = copy.deepcopy(row)
        key = row['fieldname']
        # Remove the key from the row copy
        rowdict.pop('fieldname')
        vocabdict[key]=rowdict                
    return vocabdict

def term_values_recommended(lookupdict):
    ''' Get non-standard values and their standard equivalents from a lookupdict
    parameters:
        lookupdict - dictionary of lookup terms from a vocabulary (required)
    returns:
        recommended - dictionary of verbatim values and their recommended equivalents
    '''
    functionname = 'term_values_recommended()'

    if lookupdict is None or len(lookupdict)==0:
        s = 'No lookup dictionary given in %s.' % functionname
        logging.debug(s)
        return None

    recommended = {}

    for key, value in lookupdict.items():
        if value['vetted']=='1':
            if value['standard'] != key:
                recommended[key] = value

    return recommended

def recommended_value(lookupdict, lookupvalue):
    ''' Get recommended standard value for lookupvalue from a lookup dictionary
    parameters:
        lookupdict - dictionary of lookup terms from a vocabulary. Dictionary must
            contain a key for which the value is another dictionary, and that 
            subdictionary must contain a key 'standard' for which the value is the 
            recommended value. The subdictionary may contain other keys as desired 
            (required)
    returns:
        subdictionary - dictionary containing the recommended value
    '''
    functionname = 'recommended_value()'

    if lookupdict is None or len(lookupdict)==0:
        s = 'No lookup dictionary given in %s.' % functionname
        logging.debug(s)
        return None

    if lookupvalue is None or len(lookupvalue)==0:
        s = 'No lookup value given in %s.' % functionname
        logging.debug()
        return None

    try:
        subdictionary = lookupdict[lookupvalue]
        return subdictionary
    except:
        s = '"%s" not found in lookup dictionary in %s.' % (lookupvalue, functionname)
        logging.debug(s)
        return None

def compose_dict_from_key(key, fieldlist, separator=None):
    ''' Create a dictionary from a string, a separator, and an ordered list of the names
        of the fields in the key.
    parameters:
        key - the field name that holds the distinct values in the vocabulary file
            (optional; default None)
        key - string from which to make the dict (required) (e.g., '|United States|US')
        fieldlist - ordered list of field names for the values in the key (required)
            (e.g., ['continent', 'country', 'countryCode'])
        separator - string separating the values in the key (optional; default None)
    returns:
        d - dictionary of fields and their values 
            (e.g., {'continent':'', 'country':'United States', 'countryCode':'US' } )
    '''
    functionname = 'compose_dict_from_key()'

    if key is None or len(key)==0:
        s = 'No key given in %s.' % functionname
        logging.debug(s)
        return None

    if fieldlist is None or len(fieldlist)==0:
        s = 'No term list given in %s.' % functionname
        logging.debug(s)
        return None

    if separator is None:
        vallist = key
    else:
        vallist = key.split(separator)
    i = 0
    d = {}

    for t in fieldlist:
        d[t]=vallist[i]
        i += 1

    return d

def terms_not_in_dwc(checklist, casesensitive=False):
    ''' From a list of terms, get those that are not Darwin Core terms.
    parameters:
        checklist - list of values to check against Darwin Core (required)
        casesensitive - True if the test for inclusion is case sensitive (default True)
    returns:
        a sorted list of non-Darwin Core terms from the checklist
    '''
    # No need to check if checklist is given, not_in_list() does that
    functionname = 'terms_not_in_dwc()'

    if casesensitive==True:
        return not_in_list(simpledwctermlist, checklist)

    lowerdwc = []
    for term in simpledwctermlist:
        lowerdwc.append(ustripstr(term))

    notfound = not_in_list(lowerdwc,checklist,function=ustripstr)
    return notfound

def terms_not_in_darwin_cloud(checklist, dwccloudfile, encoding=None, vetted=True, 
    casesensitive=False):
    ''' Get the list of distinct values in a checklist that are not in the Darwin Cloud
        vocabulary. Verbatim values in the Darwin Cloud vocabulary should be lower-case and
        stripped already, so that is what must be matched here. The Darwin Cloud vocabulary
        should have the case-sensitive standard value.
    parameters:
        checklist - list of values to check against the target list (required)
        dwccloudfile - the vocabulary file for the Darwin Cloud (required)
        vetted - set to False if unvetted values should also be returned (default True)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        a sorted list of distinct new values not in the Darwin Cloud vocabulary
    '''
    functionname = 'terms_not_in_darwin_cloud()'

    if checklist is None or len(checklist)==0:
        s = 'No checklist given in %s.' % functionname
        logging.debug(s)
        return None

    dialect = csv_file_dialect(dwccloudfile)
    
    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(dwccloudfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # No need to check if dwccloudfile is given and exists, vocab_dict_from_file() and
    # vetted_vocab_dict_from_file() do that.
    if vetted==True:
        darwinclouddict = vetted_vocab_dict_from_file(dwccloudfile, 'fieldname',
            dialect=dialect, encoding=encoding)
    else:
        darwinclouddict = vocab_dict_from_file(dwccloudfile, 'fieldname', 
            dialect=dialect, encoding=encoding)

    dwcloudlist = []
    for key, value in darwinclouddict.items():
        dwcloudlist.append(key)

    if casesensitive==True:
        return not_in_list(dwcloudlist, checklist)

    lowerdwclist = []
    for term in dwcloudlist:
        lowerdwclist.append(ustripstr(term))

    notfound = not_in_list(lowerdwclist, checklist, function=ustripstr)

    return notfound

def darwinize_list(termlist, dwccloudfile, namespace=None):
    ''' Translate the terms in a list to standard Darwin Core terms.
    parameters:
        termlist - list of values to translate (required)
        dwccloudfile - the vocabulary file for the Darwin Cloud (required)
        namespace - a flag to determine if the dwc: namespace should be prepended to the
            term name
    returns:
        a list with all translatable terms translated
    '''
    functionname = 'darwinize_list()'

    if termlist is None or len(termlist)==0:
        s = 'No termlist given in %s.' % functionname
        logging.debug(s)
        return None

    dialect = csv_file_dialect(dwccloudfile)

    # No need to check if dwccloudfile is given and exists, vetted_vocab_dict_from_file() 
    # does that.
    darwinclouddict = darwin_cloud_vocab_dict_from_file(dwccloudfile)

    if darwinclouddict is None:
        s = 'No Darwin Cloud terms in %s.' % functionname
        logging.debug(s)
        return None

    thelist = []
    for term in termlist:
        thelist.append(ustripstr(term))

    addnamespace = False
    if namespace is not None and 'y' in namespace:
        addnamespace = True

    darwinizedlist = []
    i = 0
    j = 1
    for term in thelist:
        if term in darwinclouddict:
            if darwinclouddict[term]['standard'] is not None and \
                len(darwinclouddict[term]['standard'].strip()) > 0:
                if addnamespace == True:
                    ns = darwinclouddict[term]['namespace']
                    newterm = ns + ':' + darwinclouddict[term]['standard']
                else:
                    newterm = darwinclouddict[term]['standard']
            else:
                newterm = termlist[i].strip()
        else:
            newterm = termlist[i].strip()
            if len(newterm) == 0:
                newterm = 'UNNAMED_COLUMN_%s' % j
                j += 1
        darwinizedlist.append(newterm)
        i += 1

    return darwinizedlist

def darwinize_dict(inputdict, dwccloudfile, namespace=False):
    ''' Translate the keys in a dict to standard Darwin Core terms.
    parameters:
        inputdict - dict with keys to translate (required)
        dwccloudfile - the vocabulary file for the Darwin Cloud (required)
        namespace - a flag to determine if the dwc: namespace should be prepended to the
            term name
    returns:
        a dict with all translatable keys translated
    '''
    functionname = 'darwinize_dict()'

    if inputdict is None or len(inputdict)==0:
        s = 'No input dict given in %s.' % functionname
        logging.debug(s)
        return None

    dialect = csv_file_dialect(dwccloudfile)

    # No need to check if dwccloudfile is given and exists, vetted_vocab_dict_from_file() 
    # does that.
    darwinclouddict = darwin_cloud_vocab_dict_from_file(dwccloudfile)

    if darwinclouddict is None:
        s = 'No Darwin Cloud terms in %s.' % functionname
        logging.debug(s)
        return None

    darwinizeddict = {}
    i = 0
    j = 1
    for term,value in inputdict.items():
        searchterm = ustripstr(term)
        if searchterm in darwinclouddict:
            if darwinclouddict[searchterm]['standard'] is not None and \
                len(darwinclouddict[searchterm]['standard'].strip()) > 0:
                if namespace == True:
                    ns = darwinclouddict[searchterm]['namespace']
                    newterm = ns + ':' + darwinclouddict[searchterm]['standard']
                else:
                    newterm = darwinclouddict[searchterm]['standard']
            else:
                newterm = searchterm.strip()
        else:
            newterm = term.strip()
            if len(newterm) == 0:
                newterm = 'UNNAMED_COLUMN_%s' % j
                j += 1
        darwinizeddict[newterm]=value
        i += 1

    return darwinizeddict

def not_in_list(targetlist, checklist, function=None, *args, **kwargs):
    ''' Get the list of distinct values in a checklist that are not in a target list.
        Optionally pass a function to use on the items in the checklist before determining
        equality.
    Example:
       not_in_list(a,b,function=ustripstr) would return all of the stripped, uppercased
       items in b that are not in a. The items in a do not have the function applied.
    parameters:
        targetlist - list to check to see if the value already exists there (required)
        checklist - list of values to check against the target list (required)
        function - function to call for each value to compare (default None)
        args - unnamed parameters to function as tuple (optional)
        kwargs - named parameters to function as dictionary (optional)
    returns:
        a sorted list of distinct new values not in the target list
    '''
    functionname = 'not_in_list()'

    if checklist is None or len(checklist)==0:
        s = 'No checklist given in %s.' % functionname
        logging.debug(s)
        return None

    if targetlist is None or len(targetlist)==0:
        s = 'No target list given in %s.' % functionname
        logging.debug(s)
        return sorted(checklist)

    newlist = []

    if function is None:
        for v in checklist:
            if v not in targetlist:
                newlist.append(v)
    else:
        for v in checklist:
            try:
                newvalue = function(v, *args, **kwargs)
            except:
                newvalue = v
            if newvalue not in targetlist:
                newlist.append(newvalue)

    if '' in newlist:
        newlist.remove('')

    return sorted(newlist)

def distinct_vocabs_to_file(vocabfile, valuelist, key, separator=None, dialect=None):
    ''' Add distinct new verbatim values from a valuelist to a vocabulary file. Always 
        write new values as utf-8.
    parameters:
        vocabfile - full path to the vocabulary file (required)
        valuelist - list of values to check for adding to the vocabulary file (required)
        key - the field or separator-separated fieldnames that hold the distinct values 
            in the vocabulary file (required)
        separator - string to use as the value separator in the string 
            (optional; default None)
        dialect - a csv.dialect object with the attributes of the vocabulary file
            (default None)
    returns:
        newvaluelist - a sorted list of distinct verbatim values added to the vocabulary
            lookup file
    '''
    functionname = 'distinct_vocabs_to_file()'
    
    if vocabfile is None or len(vocabfile.strip())==0:
        s = 'No vocab file given in %s.' % functionname
        logging.debug(s)
        return None

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(vocabfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # No need to check if valuelist is given, not_in_list() does that
    # Get the distinct verbatim values from the vocab file
    vocablist = extract_values_from_file(vocabfile, [key], separator=separator, 
        dialect=dialect, encoding='utf-8')

    # Get the values not already in the vocab file
    newvaluelist = not_in_list(vocablist, valuelist)

    if newvaluelist is None or len(newvaluelist) == 0:
        s = 'No new values found for %s in %s' % (vocabfile, functionname)
        logging.debug(s)
        return None

    if dialect is None:
        dialect = csv_file_dialect(vocabfile)

    fieldnames = vocabheader(key, separator)

    if not os.path.isfile(vocabfile):
        write_header(vocabfile, fieldnames, vocab_dialect())

    if os.path.isfile(vocabfile) == False:
        s = 'Vocab file %s not found in %s.' % (vocabfile, functionname)
        logging.debug(s)
        return None

    with open(vocabfile, 'a', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, dialect=dialect, fieldnames=fieldnames)
        for term in newvaluelist:
            row = copy.deepcopy(vocabrowdict)
            row[key] = term
            writer.writerow(row)

    s = 'Vocabulary file written to %s in %s.' % (vocabfile, functionname)
    logging.debug(s)
    return newvaluelist

def compose_key_from_row(row, fields, separator=None):
    ''' Create a string of values of terms in a dictionary separated by separator.
    parameters:
        row -  dictionary of key:value pairs (required)
            (e.g., {'country':'United States', 'countryCode':'US'} )
        fields - string of field names for values in the row from which to construct the 
            key (required)
            (e.g., 'continent|country|countryCode')
        separator - string separating the values in fields (optional; default None)
    returns:
        values - string of separator-separated values (e.g., '|United States|US')
    '''
    functionname = 'compose_key_from_row()'

    if row is None or len(row)==0:
        s = 'No row given in %s.' % functionname
        logging.debug(s)
        return None

    if fields is None or len(fields.strip())==0:
        s = 'No terms given in %s.' % functionname
        logging.debug(s)
        return None

    if separator is None:
        fieldlist = [fields]
    else:
        fieldlist = fields.split(separator)

    vallist=[]
    for t in fieldlist:
        try:
            v=row[t]
            vallist.append(v)
        except:
            vallist.append('')
    values=compose_key_from_list(vallist, separator)

    return values
