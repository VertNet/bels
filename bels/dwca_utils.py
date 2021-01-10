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
__version__ = "dwca_utils.py 2021-01-08T23:54-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/dwca_utils.py"

# This file contains common utility functions for dealing with the content of CSV and
# TXT files.

from dwca_terms import simpledwctermlist
from operator import itemgetter
from uuid import uuid1
import os.path
import re
import glob
import codecs
import logging
import csv

# Tried using cchardet, which is much faster, but it doesn't give as good results.
try:
    from chardet import UniversalDetector
except ImportError as e:
    import warnings
    s = "The chardet package is required.\n"
    s += "pip install cchardet\n"
    warnings.warn(s)

def represents_int(s):
    ''' Determine if an object represents an integer.
    parameters:
        s - the object to test
    returns:
        True if object represents an Integer, otherwise False.
    '''
    try: 
        int(s)
        return True
    except:
        return False

def get_guid(guidtype):
    ''' Create a global unique identifier of the requested type.
    parameters:
        quidtype - the type of GUID to return.
    returns:
        a string representing the GUID
    '''
    if guidtype == 'uuid':
        return str(uuid1())
    return str(uuid1())

def collapse_whitespace(s):
    ''' Create a version of an input string with sequential whitespace changed to 
        single space, then stripped of leading and trailing whitespace.
    parameters:
        s - the input string.
    returns:
        the string with whitespace collapsed.
    '''
    if s is None:
        return ''
    # change whitespaces to one space and remove leading and trailing white space.
    return re.sub(r'(\s)+', ' ', s).strip()

def ustripstr(s):
    ''' Create a stripped, uppercase version of an input string or empty string if input
        is None.
    parameters:
        s - the input string.
    returns:
        the string after uppercasing and stripping leading and trailing whitespaces.
    '''
    if s is None:
        return ''
    return s.strip().upper()

def lstripstr(s):
    ''' Create a stripped, lowercase version of an input string or empty string if input
        is None.
    parameters:
        s - the input string.
    returns:
        the string after lowercasing and stripping leading and trailing whitespaces.
    '''
    if s is None:
        return ''
    return s.strip().lower()

def setup_actor_logging(options):
    ''' Set up logging based on 'loglevel' in a dictionary.
    parameters:
        options - dictionary in which to look for loglevel (required)
    returns:
        None
    '''
    try:
        loglevel = options['loglevel']
    except:
        loglevel = None
    if loglevel is not None:
        if loglevel.upper() == 'DEBUG':
            logging.basicConfig(level=logging.DEBUG)
            logging.debug('Log level set to DEBUG')
        elif loglevel.upper() == 'INFO':        
            logging.basicConfig(level=logging.INFO)
            logging.info('Log level set to INFO')

def tsv_dialect():
    ''' Get a dialect object with tab-separated value properties.
    parameters:
        None
    returns:
        dialect - a csv.dialect object with TSV attributes
    '''
    functionname = 'tsv_dialect()'

    dialect = csv.excel_tab
    dialect.lineterminator='\r'
    dialect.delimiter='\t'
    dialect.escapechar=''
    dialect.doublequote=True
    dialect.quotechar=''
    dialect.quoting=csv.QUOTE_NONE
    dialect.skipinitialspace=True
    dialect.strict=False
    return dialect
    
def csv_dialect():
    ''' Get a dialect object with CSV properties.
    parameters:
        None
    returns:
        dialect - a csv.dialect object with CSV attributes
    '''
    functionname = 'csv_dialect()'

    dialect = csv.excel
    dialect.lineterminator='\n'
    dialect.delimiter=','
    dialect.escapechar='\\'
    dialect.doublequote=True
    dialect.quotechar='"'
    dialect.quoting=csv.QUOTE_MINIMAL
    dialect.skipinitialspace=True
    dialect.strict=False
    return dialect

def csv_file_dialect(fullpath, encoding=None):
    ''' Detect the dialect of a CSV or TXT data file.
    parameters:
        fullpath - full path to the file to process (required)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        dialect - a csv.dialect object with the detected attributes, or None
    '''
    functionname = 'csv_file_dialect()'

    if fullpath is None or len(fullpath) == 0:
        s = 'No file given in %s.' % functionname
        logging.debug(s)
        return None

    # Cannot function without an actual file where full path points
    if os.path.isfile(fullpath) == False:
        s = 'File %s not found in %s.' % (fullpath, functionname)
        logging.debug(s)
        return None

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        #print('Going in to read_header() with encoding: %s' % encoding)
        #print('for file %s' % inputfile)
        encoding = csv_file_encoding(fullpath)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Let's look at up to readto bytes from the file
    readto = 20000
    filesize = os.path.getsize(fullpath)

    if filesize < readto:
        readto = filesize

    found_doublequotes = True
    with open(fullpath, 'r', encoding=encoding) as file:
        # Try to read the specified part of the file
        try:
            buf = file.read(readto)
            # See if the buffer has any doubled double quotes in it. If so, infer that the 
            # dialect doublequote value should be true.
            if buf.find('""')>0:
                found_doublequotes = True

            # Make a determination based on existence of tabs in the buffer, as the
            # Sniffer is not particularly good at detecting TSV file formats. So, if the
            # buffer has a tab in it, let's treat it as a TSV file 
            if buf.find('\t')>0:
                return tsv_dialect()

            # Otherwise let's see what we can find invoking the Sniffer.
            # Sniffer only works well to find a delimiter using the first line
            file.seek(0)
            firstline = file.readline()
            logging.debug('Forced to use csv.Sniffer()')
            dialect = csv.Sniffer().sniff(firstline, delimiters=',\t')

            # The Sniffer doesn't always guess the line terminator correctly either
            # Let's double-check.
            if buf.find('\r\n')>0:
                dialect.lineterminator = '\r\n'
            elif buf.find('\r')>0:
                dialect.lineterminator = '\r'
            else:
                dialect.lineterminator = '\n'
        except csv.Error as e:
            # Something went wrong, so let's try to read a few lines from the beginning of 
            # the file
            try:
                file.seek(0)
                s = 'csv_file_dialect()'
                s += ' %s' % e
                s += ' Re-sniffing %s to %s' % (fullpath, readto)
                logging.debug(s)
                sample_text = ''.join(file.readline() for x in xrange(2,4,1))
                # See if the buffer has any doubled double quotes in it. If so, infer that the 
                # dialect doublequote value should be true.
                if sample_text.find('""')>0:
                    found_doublequotes = True
                dialect = csv.Sniffer().sniff(sample_text)
            # Sorry, couldn't figure it out. Let's treat it as csv
            except csv.Error as e:
                s = 'Unable to determine csv dialect in %s' % functionname
                s += ' %s' % e
                logging.debug(s)
                return csv_dialect()
    
    # Fill in some standard values for the remaining dialect attributes        
    if dialect.escapechar is None:
        dialect.escapechar = '\\'

    dialect.skipinitialspace = True
    dialect.strict = False
    dialect.doublequote = found_doublequotes
    return dialect

def dialects_equal(dialect1, dialect2):
    ''' Determine if two dialects have the same attributes.
    parameters:
        dialect1 - a csv.dialect object (required)
        dialect2 - a csv.dialect object (required)
    returns:
        True if the attributes are all the same, otherwise False
    '''
    functionname = 'dialects_equal()'

    if dialect1 is None or dialect2 is None:
        return False
    if dialect1.lineterminator != dialect2.lineterminator:
        return False
    if dialect1.delimiter != dialect2.delimiter:
        return False
    if dialect1.escapechar != dialect2.escapechar:
        return False
    if dialect1.quotechar != dialect2.quotechar:
        return False
    if dialect1.doublequote != dialect2.doublequote:
        return False
    if dialect1.quoting != dialect2.quoting:
        return False
    if dialect1.skipinitialspace != dialect2.skipinitialspace:
        return False
    if dialect1.strict != dialect2.strict:
        return False
    return True

def dialect_attributes(dialect):
    ''' Get a string showing the attributes of a csv dialect.
    parameters:
        dialect - a csv.dialect object (required)
    '''
    functionname = 'dialect_atributes()'

    if dialect is None:
        return 'No dialect given in dialect_attributes().'

    s = 'lineterminator: ' 

    if dialect.lineterminator == '\r':
        s+= '{CR}'
    elif dialect.lineterminator == '\n':
        s+= '{LF}'
    elif dialect.lineterminator == '\r\n':
        s+= '{CR}{LF}'
    else: 
        s += dialect.lineterminator

    s += '\ndelimiter: '

    if dialect.delimiter == '\t':
        s+= '{TAB}'
    else:
        s+= dialect.delimiter

    s += '\nescapechar: ' 
    s += dialect.escapechar

    s += '\ndoublequote: '

    if dialect.doublequote == True:
        s += 'True' 
    else:
        s += 'False' 

    s += '\nquotechar: ' 
    s += dialect.quotechar

    s += '\nquoting: ' 

    if dialect.quoting == csv.QUOTE_NONE:
        s += 'csv.QUOTE_NONE'
    elif dialect.quoting == csv.QUOTE_MINIMAL:
        s += 'csv.QUOTE_MINIMAL'
    elif dialect.quoting == csv.QUOTE_NONNUMERIC:
        s += 'csv.QUOTE_NONNUMERIC'
    elif dialect.quoting == csv.QUOTE_ALL:
        s += 'csv.QUOTE_ALL'

    s += '\nskipinitialspace: ' 

    if dialect.skipinitialspace == True:
        s += 'True'
    else:
        s += 'False'

    s += '\nstrict: ' 

    if dialect.strict == True:
        s += 'True'
    else:
        s += 'False'

    return s

def read_header(inputfile, dialect=None, encoding=None):
    ''' Get the header line of a CSV or TXT data file.
    parameters:
        inputfile - full path to the input file (required)
        dialect - csv.dialect object with the attributes of the input file (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        header - a list containing the fields in the original header
    '''
    functionname = 'read_header()'

    if inputfile is None or len(inputfile)==0:
        s = 'No file given in %s.' % functionname
        logging.debug(s)
        return None

    # Cannot function without an actual file where the full path points
    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    header = None

    # If no explicit dialect for the file is given, figure it out from the file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        #print('Going in to read_header() with encoding: %s' % encoding)
        #print()'for file %s' % inputfile)
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Open up the file for processing
    with open(inputfile, 'r', newline='', encoding=encoding) as data:
#        reader = csv.DictReader(utf8_data_encoder(data, encoding), dialect=dialect, 
#            encoding=encoding)
        reader = csv.DictReader(data, dialect=dialect)
        # header is the list as returned by the reader
        header=reader.fieldnames

    return header

def read_rows(inputfile, rowcount, dialect, encoding, header=True, fieldnames=None):
    ''' Read rows from a csv file. Determine the existence of the file, its dialect, and 
        its encoding before making a call to this function.
    parameters:
        inputfile - full path to the input file (required)
        rowcount - the number of rows to return
        dialect - csv.dialect object with the attributes of the input file (required)
        encoding - a string designating the input file encoding (required) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        fieldnames -  list containing the fields in the header (optional)
        header - True if the file has a header row (optional; default True)
    returns:
        rows - a list of row dictionaries
    '''
    functionname = 'read_rows()'

    rows = []
    i = 0
    with open(inputfile, 'r', newline='', encoding=encoding) as data:
        if fieldnames is None or len(fieldnames)==0:
#            reader = csv.DictReader(utf8_data_encoder(data, encoding), 
#                dialect=dialect, encoding=encoding)
            reader = csv.DictReader(data, dialect=dialect)
        else:
#            reader = csv.DictReader(utf8_data_encoder(data, encoding), \
#                dialect=dialect, encoding=encoding, fieldnames=fieldnames)
            reader = csv.DictReader(data, dialect=dialect, fieldnames=fieldnames)
            if header==True:
                next(reader)
        for row in reader:
            rows.append(row)
            i += 1
            if i == rowcount:
                return rows
    return rows

# Not needed in Python 3.x
# def UnicodeDictReader(utf8_data, **kwargs):
#     csv_reader = csv.DictReader(utf8_data, **kwargs)
#     for row in csv_reader:
#         yield {unicode(key, 'utf-8'):unicode(value, 'utf-8') for key, value in row.iteritems()}

def count_rows(inputfile):
    ''' Counts rows in a file.
    parameters:
        inputfile - full path to the input file (required)
    returns:
        count - the number of rows in the file
    '''
    functionname = 'count_rows()'

    with open(inputfile, "r") as f:
        count = sum(bl.count("\r") for bl in blocks(f))

    if count == 0:
        with open(inputfile, "r") as f:
            count = sum(bl.count("\n") for bl in blocks(f))
    return count+1

def blocks(file, size=65536):
    ''' Yield blocks of given size in bytes from file.
    parameters:
        file - full path to the input file (required)
        size - block size in bytes
    returns:
        b - the bytes in the block
    '''
    while True:
        b = file.read(size)
        if not b: break
        yield b

def composite_header(fullpath, dialect=None, encoding=None):
    ''' Get a header that includes all of the fields in headers of a set of files in the 
        given path. Files on fullpath are assumed to be in the same dialect and encoding.
    parameters:
        fullpath - full path to the files to process (required) (e.g., './*.txt')
        dialect - csv.dialect object with the attributes of the input files, which must
           all have the same dialect if dialect is given, otherwise it will be detected
           (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        compositeheader - a list containing the fields in the header
    '''
    functionname = 'composite_header()'

    if fullpath is None or len(fullpath)==0:
        s = 'No file path given in %s.' % functionname
        logging.debug(s)
        return None

    compositeheader = None
    useddialect = dialect
    usedencoding = encoding
    files = glob.glob(fullpath)

    if files is None:
        s = 'No files found on path %s in %s.' % (fullpath, functionname)
        logging.debug(s)
        return None

    for file in files:
        if dialect is None:
            useddialect = csv_file_dialect(file)
            # csv_file_dialect() always returns a dialect if there is an input file.
            # No need to check.

        # Try to determine the encoding of the inputfile.
        if encoding is None or len(encoding.strip()) == 0:
            usedencoding = csv_file_encoding(file)
            # csv_file_encoding() always returns an encoding if there is an input file.    

        header = read_header(file, useddialect, usedencoding)
        compositeheader = merge_headers(compositeheader, header)

    return compositeheader

def write_header(fullpath, fieldnames, dialect):
    ''' Write the header line of a CSV or TXT data file in utf-8 encoding.
    parameters:
        fullpath - full path to the input file (required)
        fieldnames -  list containing the fields in the header (required)
        dialect - csv.dialect object with the attributes of the output file (required)
    returns:
        True if the header was written to file, otherwise False
    '''
    functionname = 'write_header()'

    if fullpath is None or len(fullpath)==0:
        s = 'No output file given in %s.' % functionname
        logging.debug(s)
        return False

    with open(fullpath, 'w', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, dialect=dialect, fieldnames=fieldnames)
        try:
            writer.writeheader()
        except Exception as e:
            s = 'No header written to output file %s in %s.' % (fullpath, functionname)
            s += ' %s' % e
            logging.debug(s)
            return False

    return True

def header_map(header):
    ''' Construct a map between a header and a cleaned version of the header.
    parameters:
        header - list of field names to clean (required)
    returns:
        headermap - a dictionary of cleanedfield:originalfield pairs
    '''
    functionname = 'header_map()'

    if header is None or len(header)==0:
        s = 'No header given in %s.' % functionname
        logging.debug(s)
        return None

    headermap = {}

    for field in header:
        cleanfield = field.strip().lower()
        headermap[cleanfield]=field

    return headermap

def strip_list(inputlist):
    ''' Create a list of strings stripped of whitespace from strings in an input list.
    parameters:
        inputlist - list of strings (required)
    returns:
        outputlist - a list of field names after stripping
    '''
    functionaname = 'strip_list()'

    # Cannot function without an inputlist
    if inputlist is None or len(inputlist)==0:
        s = 'No list given in %s.' % functionname
        logging.debug(s)
        return None

    outputlist = []
    i=1

    # Strip each string in the inputlist and append it to the outputlist
    for field in inputlist:
        if field is not None:
            cleanstring = field.strip()
        else:
            cleanstring = ''
        if len(cleanstring)==0:
            cleanstring = 'field_%s' % i

        outputlist.append(cleanstring)
        i+=1

    return outputlist

def clean_header(header):
    ''' Construct a header from the cleaned field names in a header.
    parameters:
        header - list of field names (required)
    returns:
        cleanheader - a list of field names after cleaning
    '''
    functionname = 'clean_header()'

    # Cannot function without a header
    if header is None or len(header)==0:
        s = 'No header given in %s.' % functionname
        logging.debug(s)
        return None

    cleanheader = []
    i=1

    # Clean each field in the header and append it to the cleanheader
    for field in header:
        cleanfield = field.strip().lower()
        if len(cleanfield)==0:
            cleanfield = 'field%s' % i

        cleanheader.append(cleanfield)
        i+=1

    return cleanheader

def merge_headers(headersofar, headertoadd=None):
    ''' Construct a header from the distinct white-space-stripped fields in two headers.
    parameters:
        headersofar - first header to merge (required)
        headertoadd - header to merge with the first header (optional; default None)
    returns:
        a sorted list of fields for the combined header
    '''
    functionname = 'merge_headers()'

    if headersofar is None and headertoadd is None:
        s = 'No header given in %s.' % functionname
        logging.debug(s)
        return None

    composedheader = set()

    if headersofar is not None:
        for field in headersofar:
            addme = field.strip()
            if len(addme) > 0:
                composedheader.add(addme)

    if headertoadd is not None:
        for field in headertoadd:
            addme = field.strip()
            if len(addme) > 0:
                composedheader.add(addme)

    if len(composedheader) == 0:
        s = 'No fields in composed header in %s.' % functionname
        logging.debug(s)
        return None

    return sorted(list(composedheader))

def convert_csv(inputfile, outputfile, dialect=None, encoding=None, format=None):
    ''' Convert an arbitrary csv file into a txt file in utf-8.
    parameters:
        inputfile - full path to the input file (required)
        outputfile - full path to the converted file (required)
        dialect - csv.dialect object with the attributes of the input file (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        format - output file format (e.g., 'csv' or 'txt') (optional; default 'txt')
    returns:
        True if finished successfully, otherwise False
    '''
    functionname = 'convert_csv()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return False

    if outputfile is None or len(outputfile) == 0:
        s = 'No output file given in %s.' % functionname
        logging.debug(s)
        return False

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return False

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Create a the dialect object for the output file based on the given format
    if format is not None and format.lower() == 'csv':
        outdialect = csv_dialect()
    else:
        outdialect = tsv_dialect()

    # Get the header from the input file
    inputheader = read_header(inputfile, dialect=dialect, encoding=encoding)

    if inputheader is None:
        s = 'Unable to read header for %s in %s.' % (inputfile, functionname)
        logging.debug(s)
        return False

    with open(outputfile, 'w', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, dialect=outdialect, fieldnames=inputheader)
        writer.writeheader()

        # Iterate through all rows in the input file
        for row in read_csv_row(inputfile, dialect=dialect, encoding=encoding, 
            header=True, fieldnames=inputheader):
            writer.writerow(row)

    s = 'File written to %s in %s.' % (outputfile, functionname)
    logging.debug(s)
    return True

def csv_select_fields(inputfile, outputfile, fieldlist=None, dialect=None, encoding=None, format=None):
    ''' Write data from selected fields of an input file to an output file.
    parameters:
        inputfile - full path to the input file (required)
        outputfile - full path to the converted file (required)
        fieldlist - field names separated by | in the desired output order (default None)
        dialect - csv.dialect object with the attributes of the input file (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        format - output file format (e.g., 'csv' or 'txt') (optional; default 'txt')
    returns:
        True if finished successfully, otherwise False
    '''
    functionname = 'csv_select_fields()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return False

    if outputfile is None or len(outputfile) == 0:
        s = 'No output file given in %s.' % functionname
        logging.debug(s)
        return False

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return False

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Create a the dialect object for the output file based on the given format
    if format is not None and format.lower() == 'csv':
        outdialect = csv_dialect()
    else:
        outdialect = tsv_dialect()

    # Get the header from the input file
    inputheader = read_header(inputfile, dialect=dialect, encoding=encoding)

    if inputheader is None:
        s = 'Unable to read header for %s in %s.' % (inputfile, functionname)
        logging.debug(s)
        return False

    # Get the output header from the field list
    if fieldlist is None or len(fieldlist.strip()) == 0:
        s = 'No output fields specified in %s.' % (functionname)
        logging.debug(s)
        return False

    outputheader = fieldlist.split('|')

    with open(outputfile, 'w', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, dialect=outdialect, fieldnames=outputheader)
        writer.writeheader()

        # Iterate through all rows in the input file
        for row in read_csv_row(inputfile, dialect=dialect, encoding=encoding, 
            header=True, fieldnames=inputheader):
            for field in inputheader:
                if field not in outputheader:
                    row.pop(field)
            writer.writerow(row)

    s = 'File written to %s in %s.' % (outputfile, functionname)
    logging.debug(s)
    return True

def csv_clean_whitespace(inputfile, outputfile, dialect=None, encoding=None, format=None):
    ''' Write data from an input file to an output file where all fields have 
        sequential whitespaces replaced with a single space and leading and trailing 
        whitespaces removed.
    parameters:
        inputfile - full path to the input file (required)
        outputfile - full path to the converted file (required)
        dialect - csv.dialect object with the attributes of the input file (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        format - output file format (e.g., 'csv' or 'txt') (optional; default 'txt')
    returns:
        True if finished successfully, otherwise False
    '''
    functionname = 'csv_clean_white_space()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return False

    if outputfile is None or len(outputfile) == 0:
        s = 'No output file given in %s.' % functionname
        logging.debug(s)
        return False

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return False

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Create a the dialect object for the output file based on the given format
    if format is not None and format.lower() == 'csv':
        outdialect = csv_dialect()
    else:
        outdialect = tsv_dialect()

    # Get the header from the input file
    inputheader = read_header(inputfile, dialect=dialect, encoding=encoding)

    if inputheader is None:
        s = 'Unable to read header for %s in %s.' % (inputfile, functionname)
        logging.debug(s)
        return False

    print(dialect_attributes(dialect))

    with open(outputfile, 'w', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, dialect=outdialect, fieldnames=inputheader)
        writer.writeheader()

        # Iterate through all rows in the input file
        for row in read_csv_row(inputfile, dialect=dialect, encoding=encoding, 
            header=True, fieldnames=inputheader):
            for field in inputheader:
                row[field]=collapse_whitespace(row[field])
            print(row)
            writer.writerow(row)

    s = 'File written to %s in %s.' % (outputfile, functionname)
    logging.debug(s)
    return True

def term_rowcount_from_file(inputfile, termname, dialect=None, encoding=None):
    ''' Count of the rows that are populated for a given term.
    parameters:
        inputfile - full path to the input file (required)
        termname - term for which to count rows (required)
        dialect - csv.dialect object with the attributes of the input files (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        rowcount - the number of rows with the term populated
    '''
    functionname = 'term_rowcount_from_file()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return 0

    if termname is None or len(termname) == 0:
        s = 'No term name given in %s.' % functionname
        logging.debug(s)
        return 0

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return 0

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Search for fields based on a cleaned header
    cleanheader = clean_header(read_header(inputfile, dialect, encoding))

    # Search for term based on cleaned term to match cleaned header
    cleanterm = clean_header([termname])[0]

    if cleanterm not in cleanheader:
        s = 'Term %s not found in %s in %s.' % (termname, inputfile, functionname)
        logging.debug(s)
        return 0

    rowcount = 0

    for row in read_csv_row(inputfile, dialect, encoding, fieldnames=cleanheader):
        try:
            value = row[cleanterm]
            if value is not None and len(value.strip()) > 0:
                rowcount += 1
        except:
            pass

    return rowcount

def term_completeness_from_file(inputfile, dialect=None, encoding=None):
    ''' Make a dictionary of field names and the number of rows in which each is 
        populated.
    parameters:
        inputfile - full path to the input file (required)
        dialect - csv.dialect object with the attributes of the input files (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        fieldcountdict - dictionary of field names and the number of rows in which they 
            are populated in the inputfile
    '''
    functionname = 'term_completeness_from_file()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return 0

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return 0

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Search for fields based on a cleaned header
    header = read_header(inputfile, dialect, encoding)

    rowcount = 0

    # Set up the dictionary of row counts for the fields in the input file
    fieldcountdict = {}
    for field in header:
        fieldcountdict[field] = 0

    for row in read_csv_row(inputfile, dialect, encoding, fieldnames=header):
        for field in header:
            v = row[field]
            if v is not None and len(v.strip())>0:
                fieldcountdict[field] += 1
        rowcount += 1
    fieldcountdict['rows'] = rowcount
    return fieldcountdict

def csv_field_checker(inputfile, dialect=None, encoding=None):
    ''' Determine if any row in a csv file has fewer fields than the header.
    parameters:
        inputfile - full path to the input file (required)
        dialect - csv.dialect object with the attributes of the input files (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
    returns:
        index, row - a tuple composed of the index of the first row that has a different 
            number of fields (1 is the first row after the header) and the row string
    '''
    functionname = 'csv_field_checker()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    header = read_header(inputfile, dialect, encoding)

    if header is None:
        s = 'No header found for %s in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    delimiter = dialect.delimiter
    fieldcount = len(header)

    with open(inputfile, 'r', newline='', encoding=encoding) as f:
        i=0
        for line in f:
            delimitercount = line.count(delimiter)
            if delimitercount < fieldcount-1:
                return i, line
            i+=1

    return None

def purge_non_printing_from_file(inputfile, outputfile, dialect=None, encoding=None, 
    sub='-'):
    ''' Remove new lines and carriage returns in data. Assumes that the header is intact
        with the correct number of columns.
    parameters:
        inputfile - full path to the input file (required)
        outputfile - full path to the translated output file (required)
        dialect - csv.dialect object with the attributes of the input files (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        sub - character sequence to substitute for the non-printing character 
            (default '-')
    returns:
        False if the removal does not complete successfully, otherwise True
    '''
    functionname = 'purge_non_printing_from_file()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    header = read_header(inputfile, dialect, encoding)

    if header is None:
        s = 'No header found for %s in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    fieldcount = len(header)
    delimiter = dialect.delimiter
    previousline = ''

    with open(outputfile, 'w', encoding='utf-8') as outfile:
        with open(inputfile, 'r', newline='', encoding=encoding) as infile:
            i = 0
            for line in infile:
                line = previousline.replace('\n', sub).replace('\r', sub) + line
                delimitercount = line.count(delimiter)
                if delimitercount == fieldcount-1:
                    writethis = filter_non_printable(line, sub)
                    outfile.write(writethis)
                    previousline = ''
                elif delimitercount < fieldcount-1:
                    previousline = line
                else:
                    s = 'Unable to correctly join line %s ' % i
                    s += 'from %s in %s' % (inputfile, functionname)
                    logging.debug(s)
                    return False
                i += 1

    s = 'Output file written to %s in %s.' % (outputfile, functionname)
    logging.debug(s)
    return True

def filter_non_printable(str, sub = ''):
    ''' Create a copy of a string with non-printing characters removed.
    parameters:
        str - the input string (required)
        sub - character sequence to substitute for the non-printing character 
            (default '')
    returns:
        string with the non-printing characters removed
    '''
    functionname = 'filter_non_printable()'

    newstr = ''
    for c in str:
        if ord(c) > 31 or ord(c) == 9 or ord(c) == 10 or ord(c) == 13:
            newstr += c
        else:
            newstr += sub
    return newstr

def csv_file_encoding(inputfile, maxlines=None):
    ''' Try to discern the encoding of a file. One can use chardetect filename from the 
        command line to discern what the results of csv_file_encoding(inputfile) will be.
    parameters:
        inputfile - the full path to an input file (required)
        maxlines  - the maximum number of lines to read from the csv file(optional)
    returns:
        the best guess at an encoding, defaulting to utf-8, or None on error
    '''
    functionname = 'csv_file_encoding()'

    line_count = 0
    limitlines = False
    
    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    if represents_int(maxlines) and maxlines > 0:
        limitlines = True
             
    detector = UniversalDetector()
#    with open(inputfile, 'r', newline='') as indata:
    with open(inputfile, 'rb') as indata:
        for line in indata:
            line_count += 1
            detector.feed(line)
            if detector.done or ( limitlines and line_count >= maxlines ): break

    detector.close()
    encoding = detector.result['encoding']
    # print(encoding)

    if encoding is None or len(encoding.strip()) == 0:
        # Encoding not determined
        s = 'No appropriate encoding found for file %s. Forcing utf8 ' % inputfile
        s += 'in %s' % functionname
        logging.debug(s)
        encoding = 'utf-8'

    return encoding

def extract_values_from_file(
    inputfile, fields, separator=None, dialect=None, encoding=None, 
    function=None, *args, **kwargs):
    ''' Get a list of the values of a list of fields from a file.
    parameters:
        inputfile - full path to the input file (required)
        fields - list of fields to extract from the input file (required)
        separator - string to separate values in the output string 
            (optional: default None)
        dialect - csv.dialect object with the attributes of the input file (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        function - function to call for each value extracted (default None)
        args - unnamed parameters to function as tuple (optional)
        kwargs - named parameters to function as dictionary (optional)
    returns:
        values - the extracted values of the fields in the list, concatenated with
            separator between values
    '''
    functionname = 'extract_values_from_file()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Create a set into which to put the distinct values
    values = set()

    # Create a cleaned version of the header
    cleanheader = clean_header(read_header(inputfile, dialect, encoding))

    # Create a cleaned version of fields
    cleanfields = clean_header(fields)

    #print('cleanheader: %s' % cleanheader)
    #print('cleanfields: %s' % cleanfields)

    # Extract values from the rows in the input file
    for row in read_csv_row(inputfile, dialect, encoding, fieldnames=cleanheader):
        try:
            value = extract_values_from_row(row, cleanfields, separator)
            if value is not None:
                if function is not None:
                    newvalue = function(value, *args, **kwargs)
                    values.add(newvalue)
                else:
                    values.add(value)
        except:
            pass
    return sorted(list(values))

def extract_value_counts_from_file(
    inputfile, fields, separator=None, dialect=None, encoding=None, 
    function=None, *args, **kwargs):
    ''' Get a dictionary of values of a list of fields from a file and their counts.
    parameters:
        inputfile - full path to the input file (required)
        fields - list of fields to extract from the input file (required)
        separator - string to separate values in the output string 
            (optional; default None)
        dialect - csv.dialect object with the attributes of the input file (default None)
        encoding - a string designating the input file encoding (optional; default None) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        function - function to call for each value extracted (default None)
        args - unnamed parameters to function as tuple (optional)
        kwargs - named parameters to function as dictionary (optional)
    returns:
        values - the extracted values of the fields in the list, concatenated with
            separator between values
    '''
    functionname = 'extract_value_counts_from_file()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    

    # Create a cleaned version of the header
    cleanheader = clean_header(read_header(inputfile, dialect, encoding))

    # Create a cleaned version of fields
    cleanfields = clean_header(fields)

    # Create a set into which to put the distinct values
    values = {}

    # Extract values from the rows in the input file
    for row in read_csv_row(inputfile, dialect, encoding, fieldnames=cleanheader):
        try:
            value = extract_values_from_row(row, cleanfields, separator)
            if value is not None:
                if function is not None:
                    newvalue = function(value, *args, **kwargs)
                    if newvalue in values:
                        values[newvalue] += 1
                    else:
                        values[newvalue] = 1
                else:
                    if value in values:
                        values[value] += 1
                    else:
                        values[value] = 1
        except:
            pass
    return sorted(values.items(), key=itemgetter(1), reverse=True)

def extract_values_from_row(row, fields, separator=None):
    ''' Get the values of a list of fields from a row.
    parameters:
        row - a dictionary (required)
        fields - list of fields to extract from the row (required)
        separator - string to separate values the output string (optional; default None)
    returns:
        values - the extracted values of the fields in the list, concatenated with
            separator between values
    '''
    functionname = 'extract_values_from_row()'

    if fields is None or len(fields)==0:
        return None

    if separator is None:
        separator = ''

    values = ''

    n = 0
    for field in fields:
        try:
            value = row[field]
        except:
            value = ''

        if n==0:
            values = value
            n = 1
        else:
            values += separator+value

    if len(values) == 0:
        return None
    return values

def extract_fields_from_row(row, fields, separator=None):
    ''' Make a row from an existing dictionary, keeping only the fields names in the
        fields list.
    parameters:
        row - a dictionary (required)
        fields - list of fields to extract from the row (required)
        separator - string to separate values the output string (optional; default None)
    returns:
        newrow - the row constructed from the input row and field list
    '''
    functionname = 'extract_fields_from_row()'

    if fields is None or len(fields)==0:
        return None

    if separator is None:
        separator = ''

    newrow = {}

    n = 0
    for field in fields:
        try:
            value = row[field]
        except:
            value = ''
        newrow[field] = value

    return newrow

def read_csv_row(inputfile, dialect, encoding, header=True, fieldnames=None):
    ''' Yield a row from a csv file. Determine the existence of the file, its dialect, and 
        its encoding before making a call to this function.
    parameters:
        inputfile - full path to the input file (required)
        dialect - csv.dialect object with the attributes of the input file (required)
        encoding - a string designating the input file encoding (required) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        fieldnames -  list containing the fields in the header (optional)
        header - True if the file has a header row (optional; default True)
    returns:
        row - the row as a dictionary
    '''
    functionname = 'read_csv_row()'

    with open(inputfile, 'r', newline='', encoding=encoding) as data:
        if fieldnames is None or len(fieldnames)==0:
            reader = csv.DictReader(data, dialect=dialect)
        else:
            reader = csv.DictReader(data, dialect=dialect, fieldnames=fieldnames)
            if header==True:
                next(reader)
        for row in reader:
            yield row

def safe_read_csv_row(inputfile, dialect=None, encoding=None, header=True, fieldnames=None):
    ''' Yield a row from a csv file. This function tries to determine the existence of 
        the file, its dialect, and its encoding before opening and reading. These 
        parameters can be provided explicitly to speed up reading.
    parameters:
        inputfile - full path to the input file (required)
        dialect - csv.dialect object with the attributes of the input file (required)
        encoding - a string designating the input file encoding (required) 
            (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
        fieldnames -  list containing the fields in the header (optional)
        header - True if the file has a header row (optional; default True)
    returns:
        row - the row as a dictionary
    '''
    functionname = 'safe_read_csv_row()'

    if inputfile is None or len(inputfile) == 0:
        s = 'No input file given in %s.' % functionname
        logging.debug(s)
        return None

    if os.path.isfile(inputfile) == False:
        s = 'File %s not found in %s.' % (inputfile, functionname)
        logging.debug(s)
        return None

    # Determine the dialect of the input file
    if dialect is None:
        dialect = csv_file_dialect(inputfile)
        # csv_file_dialect() always returns a dialect if there is an input file.
        # No need to check.

    # Try to determine the encoding of the inputfile.
    if encoding is None or len(encoding.strip()) == 0:
        encoding = csv_file_encoding(inputfile)
        # csv_file_encoding() always returns an encoding if there is an input file.    
        # No need to check.
    
    with open(inputfile, 'r', newline='', encoding=encoding) as data:
        if fieldnames is None or len(fieldnames)==0:
            reader = csv.DictReader(data, dialect=dialect)
        else:
            reader = csv.DictReader(data, dialect=dialect, fieldnames=fieldnames)
            if header==True:
                next(reader)
        for row in reader:
            yield row

# Not needed in Python 3.x
# def utf8_file_encoder(inputfile, outputfile, encoding=None):
#     ''' Translate input file to utf8.
#     parameters:
#         inputfile - full path to the input file (required)
#         outputfile - full path to the translated output file (required)
#         encoding - a string designating the input file encoding (optional; default None) 
#             (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
#     returns:
#         False if the translation does not complete successfully, otherwise True
#     '''
#     functionname = 'utf8_file_encoder()'
# 
#     # Try to determine the encoding of the inputfile.
#     if encoding is None or len(encoding.strip()) == 0:
#         encoding = csv_file_encoding(inputfile)
#         # csv_file_encoding() always returns an encoding if there is an input file.    
# 
#     i = 0
#     with open(outputfile, 'w') as outdata:
#         with open(inputfile, 'r', newline='') as indata:
#             for line in indata:
#                 try:
#                     outdata.write( utf8_line_encoder(line, encoding) )
#                 except UnicodeDecodeError as e:
#                     s = 'Failed to encode line #%s:\n%s\n' % (i, line)
#                     s += 'from %s in encoding %s. ' % (inputfile, encoding)
#                     s += 'Exception: %s %s' % (e, functionname)
#                     logging.debug(s)
#                     return False
#                 i += 1
#     return True
# 
# def utf8_data_encoder(data, encoding):
#     ''' Yield a row in utf8 from a file in given encoding.
#     parameters:
#         data - the open input file (required)
#         encoding - a string designating the input file encoding (required) 
#             (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
#     returns:
#         the row in utf8
#     '''
#     functionname = 'utf8_data_encoder()'
#     for line in data:
#         try:
#             yield utf8_line_encoder(line, encoding)
#         except UnicodeDecodeError as e:
#             s = 'Failed to encode line:\n%s\n' % line
#             s += 'in encoding %s. ' % encoding
#             s += 'Exception: %s %s' % (e, functionname)
#             logging.debug(s)
# 
# Not needed in Python 3.x
# def utf8_line_encoder(line, encoding):
#     ''' Get a row with a given encoding as utf8.
#     parameters:
#         line - the line to process (required)
#         encoding - a string designating the input file encoding (required) 
#             (e.g., 'utf-8', 'mac_roman', 'latin_1', 'cp1252')
#     returns:
#         the line in utf8
#     '''
#     if encoding == 'utf-8':
#         return line
#     return line.decode(encoding).encode('utf-8')

def split_path(fullpath):
    ''' Parse out the path to, the name of, and the extension for a given file.
    parameters:
        fullpath - full path to the input file (required)
    returns:
        a tuple with the following elements:
            path - the path to the file (e.g., './')
            filext - the extension for the file name (e.g., 'thefile')
            filepattern - the file name without the path and extension (e.g., 'txt')
    '''
    functionname = 'split_path()'

    if fullpath is None or len(fullpath)==0:
        logging.debug('No input file given in split_path().')
        return None, None, None

    path = fullpath[:fullpath.rfind('/')]
    fileext = fullpath[fullpath.rfind('.')+1:]
    filepattern = fullpath[fullpath.rfind('/')+1:fullpath.rfind('.')]

    return path, fileext, filepattern

def response(returnvars, returnvals, version=None):
    ''' Create a dictionary combining keys with corresponding values.
    parameters:
        returnvars - keys to use in the output (required)
        returnvals - values for the keys in the output (required)
        version - version of the calling script
    returns:
        response - a dictionary of combined keys and values
    '''
    functionname = 'response()'

    response = {}
    i=0

    for a in returnvars:
        response[a]= returnvals[i] 
        i+=1
    if version is not None and len(version.strip())>0:
        response['version']=version

    return response

def dwc_ordered_header(header):
    ''' Construct a header with terms ordered in the Darwin Core term order.
    parameters:
        None
    returns:
        orderedheader -  Darwin Core-ordered list of field names in the given header
    '''
    functionname = 'dwc_ordered_header()'

    if header is None or len(header) == 0:
        return None

    # Make a lower case version of the header to compare against
    compare = []
    for term in header:
        compare.append(term.lower())

    # Search through the Simple Darwin terms in order and add any found in the input 
    # to an ordered header
    orderedheader = []
    for term in simpledwctermlist:
        if term.lower() in compare:
            orderedheader.append(term)

    # Add any remaining fields from the input header to the ordered header
    for term in header:
        if term not in orderedheader:
            orderedheader.append(term)

    return orderedheader

def lower_dict_keys(inputdict):
    ''' Make the keys in a dict lowercase.
    parameters:
        inputdict - the dict for which to make the keys lowercase
    returns:
        lowereddict -  the dict with keys made lowercase
    '''
    functionname = 'lower_dict_keys()'

    lowereddict = {}
    for key,value in inputdict.items():
        lowereddict[lstripstr(key)]=value
    return lowereddict
