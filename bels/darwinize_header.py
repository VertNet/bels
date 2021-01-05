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
__copyright__ = "Copyright 2020 Rauthiflor LLC"
__version__ = "darwinize_header.py 2020-12-22T08:41-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/darwinize_header.py"

from dwca_vocab_utils import darwinize_list
from dwca_utils import read_header
from dwca_utils import write_header
from dwca_utils import read_csv_row
from dwca_utils import csv_file_dialect
from dwca_utils import csv_file_encoding
from dwca_utils import csv_dialect
from dwca_utils import tsv_dialect
from dwca_utils import response
from dwca_utils import setup_actor_logging
import os
import logging
import argparse
import csv

def darwinize_header(options):
    ''' Translate field names from input file to Darwin Core field names in outputfile
        using a Darwin Cloud vocabulary lookup.
    options - a dictionary of parameters
        loglevel - level at which to log (e.g., DEBUG) (optional)
        workspace - path to a directory for the outputfile (optional)
        inputfile - full path to the input file (required)
        dwccloudfile - full path to the vocabulary file containing the Darwin Cloud 
           terms (required)
        outputfile - name of the output file, without path (required)
        encoding - string signifying the encoding of the input file. If known, it speeds
            up processing a great deal. (optional; default None) (e.g., 'utf-8')
        format - output file format (e.g., 'csv' or 'txt') (optional; default 'txt')
        namespace - prepend namespace to fields that were darwinized 
        (optional; default 'no') (e.g., 'y', 'n')
    returns a dictionary with information about the results
        outputfile - actual full path to the output file
        success - True if process completed successfully, otherwise False
        message - an explanation of the reason if success=False
    '''
    #print('%s options: %s' % (__version__, options))

    setup_actor_logging(options)

    logging.debug( 'Started %s' % __version__ )
    logging.debug( 'options: %s' % options )

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
    dwccloudfile = None
    outputfile = None
    encoding = None
    namespace = 'n'
    format = None

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

    if os.path.isfile(inputfile) == False:
        message = 'Input file %s not found. %s' % (inputfile, __version__)
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    try:
        dwccloudfile = options['dwccloudfile']
    except:
        pass

    if dwccloudfile is None or len(dwccloudfile)==0:
        message = 'No Darwin Cloud vocabulary file given. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    if os.path.isfile(dwccloudfile) == False:
        message = 'Darwin Cloud vocabulary file not found. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    try:
        outputfile = options['outputfile']
    except:
        pass

    if outputfile is None or len(outputfile)==0:
        message = 'No output file given. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    outputfile = '%s/%s' % (workspace.rstrip('/'), outputfile)

    try:
        encoding = options['encoding']
    except:
        pass

    if encoding is None or len(encoding.strip())==0:
        encoding = csv_file_encoding(inputfile)
    try:
        namespace = options['namespace']
    except:
        pass

    inputdialect = csv_file_dialect(inputfile)

    try:
        format = options['format']
    except:
        pass

    if format is None or len(format)==0:
        outputdialect = inputdialect
    elif format.lower()=='csv':
        outputdialect = csv_dialect()
    else:
        outputdialect = tsv_dialect()

    header = read_header(inputfile, dialect=inputdialect, encoding=encoding)
    dwcheader = darwinize_list(header, dwccloudfile, namespace)

    print(header)
    print(dwcheader)
    
    if dwcheader is None:
        message = 'Unable to create darwinized header. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)
        
    # Write the new header to the outputfile
    if write_header(outputfile, dwcheader, dialect=outputdialect) == False:
        message = 'Unable to write header to output file. %s' % __version__
        returnvals = [workspace, outputfile, success, message, artifacts]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    # Read the rows of the input file, append them to the output file after the 
    # header with columns in the same order.
    with open(outputfile, 'a', encoding=encoding) as outfile:
        writer = csv.DictWriter(outfile, dialect=outputdialect, fieldnames=header)
        for row in read_csv_row(inputfile, inputdialect, encoding):
            writer.writerow(row)
            #print('row: %s' % row)

    success = True
    artifacts['darwinized_header_file'] = outputfile
    returnvals = [workspace, outputfile, success, message, artifacts]
    logging.debug('Finishing %s' % __version__)
    return response(returnvars, returnvals)
	
def _getoptions():
    ''' Parse command line options and return them.'''
    parser = argparse.ArgumentParser()

    help = 'directory for the output file (optional)'
    parser.add_argument("-w", "--workspace", help=help)

    help = 'full path to the input file (required)'
    parser.add_argument("-i", "--inputfile", help=help)

    help = 'full path to the Darwin Cloud vocabulary file (required)'
    parser.add_argument("-v", "--dwccloudfile", help=help)

    help = 'output file name, no path (optional)'
    parser.add_argument("-o", "--outputfile", help=help)

    help = 'include namespace (optional; default No)'
    parser.add_argument("-n", "--namespace", help=help)

    help = 'report file format (e.g., csv or txt) (optional; default unchanged)'
    parser.add_argument("-f", "--format", help=help)

    help = 'log level (e.g., DEBUG, WARNING, INFO) (optional)'
    parser.add_argument("-l", "--loglevel", help=help)

    return parser.parse_args()

def main():
    options = _getoptions()
    optdict = {}

    if options.inputfile is None or len(options.inputfile)==0 or \
        options.outputfile is None or len(options.outputfile)==0:
        s =  'syntax:\n'
        s += 'python darwinize_header.py'
        s += ' -w ./workspace'
        s += ' -v ./data/vocabularies/darwin_cloud.txt'
        s += ' -o darwinized.csv'
        s += ' -i ./data/tests/test_eight_specimen_records.csv'
        s += ' -n yes'
        s += ' -f csv'
        s += ' -l DEBUG'
        print('%s' % s)
        return

    optdict['workspace'] = options.workspace
    optdict['inputfile'] = options.inputfile
    optdict['dwccloudfile'] = options.dwccloudfile
    optdict['outputfile'] = options.outputfile
    optdict['namespace'] = options.namespace
    optdict['format'] = options.format
    optdict['loglevel'] = options.loglevel
    print('optdict: %s' % optdict)

    # Append distinct new field names to Darwin Cloud vocab file
    response=darwinize_header(optdict)
    print('\nresponse: %s' % response)

if __name__ == '__main__':
    main()
