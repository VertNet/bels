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

__author__ = "Marie-Elise Lecoq"
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__filename__ = 'job.py'
__version__ = __filename__ + ' ' + "2021-07-24T18:36-3:00"

import base64
import json
import csv
import io
import os
import tempfile
import logging
import re
import time

from contextlib import contextmanager
from .id_utils import dwc_location_hash, location_match_str, super_simplify
from .dwca_utils import safe_read_csv_row, lower_dict_keys
from .bels_query import get_location_by_hashid, row_as_dict
from .bels_query import get_best_sans_coords_georef_reduced
from .bels_query import import_table
from .bels_query import export_table
from .bels_query import delete_table
from .bels_query import process_import_table
from .bels_query import bigquerify_header
from .dwca_vocab_utils import darwinize_dict
from .dwca_vocab_utils import darwinize_list
from .dwca_terms import locationmatchsanscoordstermlist
from google.cloud import bigquery
from google.cloud import storage

PROJECT_ID = 'localityservice'
OUTPUT_LOCATION = 'bels_output'

def process_csv_in_bulk(event, context):
    """Background Cloud Function to be triggered by Pub/Sub to georeference all the rows
       in a CSV file by uploading the file to Cloud Storage and then loading that into
       a BigQuery table for processing.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    import base64

    logging.basicConfig(level=logging.DEBUG)

    starttime = time.perf_counter()
    
    if 'data' not in event:
        raise ValueError('no data provided')

    config = base64.b64decode(event['data']).decode('utf-8')
    json_config = json.loads(config)
    logging.debug(f'json_config going into job.py: {json_config}')
    json_config = json_config['data']

    # Google Cloud Storage location of input file
    upload_file_url = json_config['upload_file_url'] 

    # User-provided Output file name
    output_filename = json_config['output_filename'] 
    email = json_config['email']
    header = json_config['header']

#    print(f'output_filename before: {output_filename}')
#    print(f'upload_file_url: {upload_file_url}')
    # Alter output file name to [filename]-[UUID of input file location].csv
    upload_file_parts = upload_file_url.split('/')
    file_suffix = upload_file_parts[len(upload_file_parts)-1].split('.')[0]

    # Don't allow any of the following characters in output file names, substitute '_'
    output_filename = re.sub(r'[ ~`!@#$%^&*()_+={\[}\]|\\:;"<,>?\'/]', '_', output_filename)

    output_filename = '%s-%s.csv.gz' % (output_filename, file_suffix)
#    print(f'output_filename after: {output_filename}')

    # Darwinize the header
    vocabpath = './bels/vocabularies/'
    dwccloudfile = vocabpath + 'darwin_cloud.txt'
#    print(f'raw fields: {header}')
    logging.debug(f'header: {header}')
    logging.debug(f'dwccloudfile: {dwccloudfile}')
    darwinized_header = darwinize_list(header,dwccloudfile)
    
    # Make sure the field names satisfy BigQuery field name requirements
    bigqueryized_header = bigquerify_header(darwinized_header)
    
    preptime = time.perf_counter() - starttime
    msg = f'Prep time = {preptime:1.3f}s\n'
    logging.info(f'{msg}')
#    print(msg)

    bq_client = bigquery.Client()

    # Load the file into BigQuery table making table name from source file name by default
#    table_id = import_table(bq_client, upload_file_url, checked_header)
    logging.debug(f'Prepping for import_table. upload_file_url: {upload_file_url}')
    logging.debug(f'darwinized_header: {darwinized_header}')
    table_id = import_table(bq_client, upload_file_url, darwinized_header)
    # print(f'process_csv_in_bulk() table_id: {table_id}')
    importtime = time.perf_counter()-preptime
    msg = f'Import time = {importtime:1.3f}s\n'
    logging.info(msg)
#    print(msg)

    # Do georeferencing on the imported table with SQL script
    output_table_id = process_import_table(bq_client, table_id)
    # print(f'process_csv_in_bulk() output_table_id: {output_table_id}')
    georeftime = time.perf_counter()-importtime
    msg = f'Georef time = {georeftime:1.3f}s\n'
    logging.info(msg)
#    print(msg)

    # Export results to Google Cloud Storage
    # Make this work for big files that get split
    destination_uri = f'gs://{PROJECT_ID}/{OUTPUT_LOCATION}/{output_filename}'

    logging.debug(f'Prepping for export_table. destination_uri: {destination_uri}')
    logging.debug(f'output_table_id: {output_table_id}')
    outputfilelist = export_table(bq_client, output_table_id, destination_uri)
    exporttime = time.perf_counter()-georeftime
    print(f'outputfilelist: {outputfilelist}')

    # Output is already public. The following is not necessary.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(PROJECT_ID)
#    blob = bucket.blob(f'{OUTPUT_LOCATION}/{output_filename}')
#    blob.make_public()
#    output_url = blob.public_url

    output_url_list = []
    for file in outputfilelist:
        blob = bucket.blob(f'{file}')
        print(f'For file {file}, blob {blob}')
        output_url_list.append(blob.public_url)
    output_url_list.sort()
#    print(f'sorted output_url_list: {output_url_list}')
    # Remove the georefs table from BigQuery
#     try:
#         bq_client.get_table(table_id)
#         delete_table(bq_client, table_id)
#         try:
#             bq_client.get_table(table_id)
#         except Exception as e:
#             print(f'Table {table_id} was deleted.')
#     except Exception as e:
#         print(f'Table {table_id} does not exist.')

    # Notify the receiving party by email given.
    try:
        send_email(email, output_url_list)
    except Exception as e:
        print(f'Error sending email: {e}. Files stored at {output_url_list}')

    elapsedtime = time.perf_counter()-starttime
    msg = []
    msg.append(f'Export time = {exporttime:1.3f}s\n')
    msg.append(f'Total elapsed time = {elapsedtime:1.3f}s')
    logging.info(''.join(msg))
#    print(''.join(msg))

# def process_csv(event, context):
#     """Background Cloud Function to be triggered by Pub/Sub.
#     Args:
#          event (dict):  The dictionary with data specific to this type of
#          event. The `data` field contains the PubsubMessage message. The
#          `attributes` field will contain custom attributes if there are any.
#          context (google.cloud.functions.Context): The Cloud Functions event
#          metadata. The `event_id` field contains the Pub/Sub message ID. The
#          `timestamp` field contains the publish time.
#     """
#     import base64
#     s = []
#     s.append(f'This Function was triggered by messageId {context.event_id} ')
#     logging.info(''.join(s))
# 
#     if 'data' not in event:
#         raise ValueError('no data provided')
# 
#     config = base64.b64decode(event['data']).decode('utf-8')
#     json_config = json.loads(config)
#     json_config = json_config['data']
#     file_url = json_config['file_url'] # Google Cloud Storage location of input file
#     filename = json_config['filename'] # User-provided Output file name
#     email = json_config['email']
#     header = json_config['header']
# 
#     # Alter output file name to [filename]-[UUID of input file location].csv
#     filename = '%s-%s.csv' % (filename, file_url.split('/')[1])
# 
#     # Don't allow any of the following characters in output file names, substitute '_'
#     filename = re.sub(r'[ ~`!@#$%^&*()_+={\[}\]|\\:;"<,>?\'/]', '_', filename)
# 
#     print(f'process_csv() file_url: {file_url}')
#     print(f'process_csv() filename: {filename}')
#     print(f'process_csv() header: {header}')
#     print(f'process_csv() email: {email}')
# 
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket(PROJECT_ID)
#     blob = bucket.get_blob(file_url)
#     bq_client = bigquery.Client()
#     with temp_file() as name:
#         blob.download_to_filename(name)
#         #blob.delete() # Do not leak documents in storage
#         return_list = find_best_georef(bq_client, name)
# 
#     output = create_output(return_list)
#     blob = bucket.blob('output/' + filename)
#     blob.upload_from_string(output, content_type='application/csv')
#     blob.make_public()
#     output_url = blob.public_url
#     print(f'process_csv() output_url: {output_url}')
# 
#     # Store that output
#     try:
#         send_email(email, output_url)
#     except Exception as e:
#         print(f'Error sending email: {e}. File stored at {output_url}')

def find_best_georef(client, filename):
    # vocabpath is the location of vocabulary files to test with
    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, './vocabularies/')

    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    
    listToCsv = []
    # logging.info('find_best_georef() filename: %s' % filename)
    for row in safe_read_csv_row(filename):
        # logging.info(f'row: {row}')
        rowdict = row_as_dict(row)

        loc = darwinize_dict(row_as_dict(row), darwincloudfile)
        lowerloc = lower_dict_keys(loc)

        sanscoordslocmatchstr = location_match_str(locationmatchsanscoordstermlist, \
            lowerloc)
        matchstr = super_simplify(sanscoordslocmatchstr)

#         s = []
#         s.append(__version__)
#         s.append(f' getting best_georef for: {matchstr}'
#         print(''.join(s))

        result = get_best_sans_coords_georef_reduced(client, matchstr)
        if result:
            for field in ['dwc_location_hash', 'locationid']:
                if field in result:
                    result[field] = base64.b64encode(result[field]).decode('utf-8')
            row.update(result)
#             s = []
#             s.append(__version__)
#             s.append(f' best_georef: {row}'
#             print(''.join(s))
        else:
            # Create a dict of empty results for all results fields anyway to make sure
            # the header is correct even if the first result is empty.
#             s = []
#             s.append(__version__)
#             s.append(f' no georef found for: {matchstr}'
#             print(''.join(s))
            pass

        listToCsv.append(row)

    return listToCsv

def create_output(occurrences):
    fieldnames = occurrences[0].keys()
    output_file = io.StringIO()

    dict_writer = csv.DictWriter(output_file, fieldnames)
    dict_writer.writeheader()
    dict_writer.writerows(occurrences)

    return output_file.getvalue()

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')

def send_email(target, output_url_list):
    import sendgrid
    from sendgrid.helpers.mail import Email, To, Content, Mail

    sg = sendgrid.SendGridAPIClient(SENDGRID_API_KEY)
    from_email = Email("vertnetinfo@vertnet.org")
    to_email = To(target)
    subject = "File ready for download from Biodiversity Enhanced Location Services (BELS)"
    s = []
    s.append('A request to process a file uploaded to BELS to find the best existing ')
    s.append('georeferences and send the results to this email address has been ')
    s.append('completed. Following is a list of URLs where the results can be ')
    s.append('downloaded. If there is more than one file in the list, it is because ')
    s.append('the size of the result exceeded the size for a single file export. In ')
    s.append('this case only the first file on the list contains the column header. ')
    s.append('Files will remain accessible at this location for a period of at least ')
    s.append('30 days.\n')
    for url in output_url_list:
        s.append(f'{url}\n')
    content = Content("text/plain", ''.join(s))
    message = Mail(from_email, to_email, subject, content)

    sg.client.mail.send.post(request_body=message.get())

@contextmanager
def temp_file():
    _, name = tempfile.mkstemp()
    try:
        yield name
    finally:
        os.remove(name)
