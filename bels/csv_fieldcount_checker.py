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
__version__ = "csv_fieldcount_checker.py 2021-02-12T15:04-03:00"
__adapted_from__ = "https://github.com/kurator-org/kurator-validation/blob/master/packages/kurator_dwca/csv_fieldcount_checker.py"

from bels.dwca_utils import csv_field_checker
from bels.dwca_utils import response
from bels.dwca_utils import setup_actor_logging
import os
import logging
import argparse

def csv_fieldcount_checker(options):
    ''' Get the first row in a csv file where the number of fields is less than the number
        of fields in the header.
    options - a dictionary of parameters
        loglevel - level at which to log (e.g., DEBUG) (optional)
        workspace - path to a directory for the output artifacts (optional)
        inputfile - full path to the input file (required)
    returns a dictionary with information about the results
        workspace - path to a directory for the output artifacts
        firstbadrowindex - the line number of the first row in the inputfile where the 
            field count does not match
        row - the content of the first line in the inputfile where the field count does
            not match.
        success - True if process completed successfully, otherwise False
        message - an explanation of the reason if success=False
    '''
    #print('%s options: %s' % (__version__, options))

    setup_actor_logging(options)

    logging.debug( 'Started %s' % __version__ )
    logging.debug( 'options: %s' % options )

    # Make a list for the response
    returnvars = ['workspace', 'firstbadrowindex', 'row', 'success', 'message']

    ### Standard outputs ###
    success = False
    message = None

    ### Custom outputs ###
    firstbadrowindex = 0
    row = None

    ### Establish variables ###
    workspace = './'
    inputfile = None

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
        returnvals = [workspace, firstbadrowindex, row, success, message]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    if os.path.isfile(inputfile) == False:
        message = 'Input file %s not found. %s' % (inputfile, __version__)
        returnvals = [workspace, firstbadrowindex, row, success, message]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    result = csv_field_checker(inputfile)

    if result is not None:
        firstbadrowindex = result[0]
        row = result[1]
        message = 'Row with incorrect number fields found. %s' % __version__
        returnvals = [workspace, firstbadrowindex, row, success, message]
        logging.debug('message:\n%s' % message)
        return response(returnvars, returnvals)

    success = True
    returnvals = [workspace, firstbadrowindex, row, success, message]
    logging.info('Finishing %s' % __version__)
    return response(returnvars, returnvals)

def _getoptions():
    ''' Parse command line options and return them.'''
    parser = argparse.ArgumentParser()

    help = 'full path to the input file'
    parser.add_argument("-i", "--inputfile", help=help)

    help = 'log level (e.g., DEBUG, WARNING, INFO) (optional)'
    parser.add_argument("-l", "--loglevel", help=help)

    return parser.parse_args()

def main():
    options = _getoptions()
    optdict = {}

    if options.inputfile is None or len(options.inputfile)==0:
        s =  'syntax:\n'
        s += 'python csv_fieldcount_checker.py'
        s += ' -i ./data/tests/test_bad_fieldcount1.txt'
        s += ' -l DEBUG'
        print('%s' % s)
        return

    optdict['inputfile'] = options.inputfile
    print('optdict: %s' % optdict)

    # Check if any rows do not have fields matching header field count
    response=csv_fieldcount_checker(optdict)
    print('\nresponse: %s' % response)

if __name__ == '__main__':
    main()
