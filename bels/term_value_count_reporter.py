#!/usr/bin/env python
# -*- coding: utf8 -*-

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
__copyright__ = "Copyright 2017 President and Fellows of Harvard College"
__version__ = "term_value_count_reporter.py 2021-12-14T20:06-03:00"
__kurator_content_type__ = "actor"
__adapted_from__ = "actor_template.py"

from dwca_utils import response
from dwca_utils import setup_actor_logging
from dwca_utils import write_header
from dwca_utils import csv_dialect
from dwca_utils import tsv_dialect
from dwca_utils import extract_value_counts_from_file
import logging
import os
import uuid
import argparse
import csv

# Replace the system csv with unicodecsv. All invocations of csv will use unicodecsv,
# which supports reading and writing unicode streams.
try:
    import unicodecsv as csv
except ImportError:
    import warnings
    s = "The unicodecsv package is required.\n"
    s += "pip install unicodecsv\n"
    s += "$JYTHON_HOME/bin/pip install unicodecsv"
    warnings.warn(s)

def term_value_count_reporter(options):
    ''' Extract a list of the distinct values of a given term in a text file along with 
        the number of times each occurs.
    options - a dictionary of parameters
        loglevel - level at which to log (e.g., DEBUG) (optional)
        workspace - path to a directory for the tsvfile (optional)
        inputfile - full path to the input file (required)
        outputfile - name of the output file, without path (optional)
        format - output file format (e.g., 'csv' or 'txt') (optional; default 'csv')
        termlist - list of fields in the field combination to count (required)
        separator - string that separates the values in in the output (e.g., '|') 
            (optional; default '|')
        encoding - string signifying the encoding of the input file. If known, it speeds
            up processing a great deal. (optional; default None) (e.g., 'utf-8')
    returns a dictionary with information about the results
        workspace - actual path to the directory where the outputfile was written
        outputfile - actual full path to the output tsv file
        success - True if process completed successfully, otherwise False
        message - an explanation of the reason if success=False
        artifacts - a dictionary of persistent objects created
    '''
    #print(f'{__version__} options: {options}')

    setup_actor_logging(options)

    #logging.debug( 'Started %s' % __version__ )
    #logging.debug( 'options: %s' % options )

    # Make a list for the response
    returnvars = ['workspace', 'outputfile', 'success', 'message', 'artifacts']

    ### Standard outputs ###
    success = False
    message = None
    # Make a dictionary for artifacts left behind
    artifacts = {}

    ### Establish variables ###
    workspace = './'
    inputfile = None
    outputfile = None
    format = None
    termlist = None
    separator = '|'
    encoding = None

    ### Required inputs ###
    try:
        workspace = options['workspace']
    except:
        pass

    try:
        inputfile = options['inputfile']
    except:
        pass

    if inputfile is None or len(inputfile)==0:
        message = 'No input file given. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    # Look to see if the input file is at the absolute path or in the workspace.
    if os.path.isfile(inputfile) == False:
        if os.path.isfile(workspace+'/'+inputfile) == True:
            inputfile = workspace+'/'+inputfile
        else:
            message = 'Input file %s not found. %s' % (inputfile, __version__)
            returnvals = [workspace, outputfile, success, message, artifacts]
            logging.debug('message:\n%s' % message)
            return response(returnvars, returnvals)

    try:
        termlist = options['termlist']
    except:
        pass

    if termlist is None or len(termlist)==0:
        message = 'No field list given. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message: %s' % message)
        return response(returnvars, returnvals)

    ### Optional inputs ###
    try:
        separator = options['separator']
    except:
        pass

    try:
        format = options['format']
    except:
        pass

    if format is None:
        format = 'csv'

    try:
        encoding = options['encoding']
    except:
        pass

    try:
        outputfile = options['outputfile']
    except:
        pass

    rootname = ''
    termname = ''
    n = 0
    for f in termlist:
        if n == 0:
            rootname += f
            termname += f
            n = 1
        else:
            rootname += '_'+f
            termname += separator+f
    if outputfile is None or len(outputfile)==0:
        outputfile = '%s_count_report_%s.%s' % (rootname, str(uuid.uuid1()), format)

    outputfile = '%s/%s' % (workspace.rstrip('/'), outputfile)

    # Get the list of values for the field given by termname along with their counts.
    counts = extract_value_counts_from_file(inputfile, termlist, separator=separator, 
        encoding=encoding)
    #print(f'counts: {counts}')

    #Try to create the report for the term value counts.
    success = term_value_count_report(outputfile, counts, termname=termname, format=format)

    if success==False:
        message = 'No count report created for %s from %s. ' % (termname, outputfile)
        message += '%s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message: %s' % message)
        return response(returnvars, returnvals)
    
    s = '%s_count_report_file' % rootname
    artifacts[s] = outputfile
    returnvals = [workspace, outputfile, success, message, artifacts]
    logging.debug('Finishing %s' % __version__)
    return response(returnvars, returnvals)

def term_value_count_report(reportfile, termcountlist, termname='value', format=None):
    ''' Write a report of the counts of values for the term.
    parameters:
        reportfile - full path to the output report file
        termcountlist - list of terms with counts (required)
        termname - name of the term for which counts were made (optional; default 'value')
        format - string signifying the csv.dialect of the report file ('csv' or 'txt')
    returns:
        success - True if report was written or if there is nothing to write, else False
    '''
    functionname = 'term_value_count_report()'

    if reportfile is None or len(reportfile)==0:
        s = 'No report file given in %s.' % functionname
        logging.debug(s)
        return False

    if termcountlist is None or len(termcountlist)==0:
        s = 'No term count list given in %s.' % functionname
        logging.debug(s)
        return True

    if format=='csv' or format is None:
        dialect = csv_dialect()
    else:
        dialect = tsv_dialect()

    countreporttermlist = [termname, 'count']

    # Create the outputfile and write the new header to it
    write_header(reportfile, countreporttermlist, dialect)

    if os.path.isfile(reportfile) == False:
        s = 'reportfile: %s not created in %s' % (reportfile, functionname)
        logging.debug(s)
        return False

    with open(reportfile, 'a') as csvfile:
        writer = csv.DictWriter(csvfile, dialect=dialect, \
            fieldnames=countreporttermlist)
        for item in termcountlist:
            writer.writerow({termname:item[0], 'count':item[1] })
    return True

def _getoptions():
    ''' Parse command line options and return them.'''
    parser = argparse.ArgumentParser()

    help = 'directory for the output file (optional)'
    parser.add_argument("-w", "--workspace", help=help)

    help = 'full path to the input file (required)'
    parser.add_argument("-i", "--inputfile", help=help)

    help = 'output file name, no path (optional)'
    parser.add_argument("-o", "--outputfile", help=help)

    help = 'report file format (e.g., csv or txt) (optional)'
    parser.add_argument("-f", "--format", help=help)

    help = "list of terms to count (required)"
    parser.add_argument("-t", "--termlist", help=help)

    help = "separator (optional)"
    parser.add_argument("-s", "--separator", help=help)

    help = "encoding (optional)"
    parser.add_argument("-e", "--encoding", help=help)

    help = 'log level (e.g., DEBUG, WARNING, INFO) (optional)'
    parser.add_argument("-l", "--loglevel", help=help)

    return parser.parse_args()

def main():
    options = _getoptions()
    optdict = {}

    if options.inputfile is None or len(options.inputfile)==0 or \
       options.termlist is None or len(options.termlist)==0:
        s =  'Single field syntax:\n'
        s += 'python term_value_count_reporter.py'
        s += ' -w ./workspace'
        s += ' -i ./data/eight_specimen_records.csv'
        s += ' -o testtermcountout.txt'
        s += ' -f csv'
        s += ' -t year'
        print(s)

        s =  'Multiple field syntax:\n'
        s += 'python term_value_count_reporter.py'
        s += ' -w ./workspace'
        s += ' -i ./data/eight_specimen_records.csv'
        s += ' -o testtermcountout.txt'
        s += ' -f txt'
        s += ' -t "country|stateprovince"'
        s += ' -s "|"'
        s += ' -e utf-8'
        s += ' -l DEBUG'
        print(s)
        return

    if options.termlist is not None:
        termstring = options.termlist
        separator = options.separator
        if separator is None:
            separator = '|'
        termlist = termstring.split(separator)

    optdict['workspace'] = options.workspace
    optdict['inputfile'] = options.inputfile
    optdict['outputfile'] = options.outputfile
    optdict['format'] = options.format
    optdict['termlist'] = termlist
    optdict['separator'] = options.separator
    optdict['encoding'] = options.encoding
    optdict['loglevel'] = options.loglevel
    print(f'optdict: {optdict}')

    # Get distinct values of termname from inputfile
    response=term_value_count_reporter(optdict)
    print(f'\nresponse: {response}')

if __name__ == '__main__':
    main()
