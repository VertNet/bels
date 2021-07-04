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
__version__ = "job.py 2021-07-04T01:35-3:00"

import base64
import json
import csv
import io
import os
import tempfile
import logging
import re

from contextlib import contextmanager
from id_utils import dwc_location_hash, location_match_str, super_simplify
from dwca_utils import safe_read_csv_row, lower_dict_keys
from bels_query import get_location_by_hashid, row_as_dict
from bels_query import get_best_sans_coords_georef_reduced
from bels_query import import_table
from bels_query import export_table
from bels_query import delete_table
from bels_query import process_import_table
from dwca_vocab_utils import darwinize_dict
from bels.dwca_vocab_utils import darwinize_list
from dwca_terms import locationmatchsanscoordstermlist
from google.cloud import bigquery
from google.cloud import storage

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

    if 'data' not in event:
        raise ValueError('no data provided')

    config = base64.b64decode(event['data']).decode('utf-8')
    json_config = json.loads(config)
    json_config = json_config['data']

    # Google Cloud Storage location of input file
    upload_file_url = json_config['upload_file_url'] 

    # User-provided Output file name
    output_filename = json_config['output_filename'] 
    email = json_config['email']
    header = json_config['header']

#    print('output_filename before: %s' % (output_filename))
#    print('upload_file_url: %s' % (upload_file_url))
    # Alter output file name to [filename]-[UUID of input file location].csv
    upload_file_parts = upload_file_url.split('/')
    file_suffix = upload_file_parts[len(upload_file_parts)-1].split('.')[0]
    output_filename = '%s-%s.csv' % (output_filename, file_suffix)
#    print('output_filename after: %s' % (output_filename))

    # Don't allow any of the following characters in output file names, substitute '_'
    output_filename = re.sub(r'[ ~`!@#$%^&*()_+={\[}\]|\\:;"<,>?\'/]', '_', output_filename)

#    print('process_csv_in_bulk() upload_file_url: %s' % upload_file_url)
#    print('process_csv_in_bulk() output_filename: %s' % output_filename)
#    print('process_csv_in_bulk() header: %s' % header)
#    print('process_csv_in_bulk() email: %s' % email)

    # Darwinize the header
    vocabpath = '../vocabularies/'
    dwccloudfile = vocabpath + 'darwin_cloud.txt'
#    print('raw fields: %s' % header)
    darwinized_header = darwinize_list(header,dwccloudfile)
#    print('dwc fields: %s' % darwinized_header)

    # Set up the blob where the uploaded file is located
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket('localityservice')
#     uploaded_blob = bucket.get_blob(file_url)
    bq_client = bigquery.Client()

    # Slow, one, by one calls to the BigQuery API
#     with temp_file() as name:
#         blob.download_to_filename(name)
#         #blob.delete() # Do not leak documents in storage
#         return_list = find_best_georef(bq_client, name)

    # Faster
    # Load the file into BigQuery table making table name from source file name by default
    table_id = import_table(bq_client, upload_file_url, darwinized_header)
    # print('process_csv_in_bulk() table_id: %s' % table_id)
    
    # Do georeferencing on the imported table with SQL script
    output_table_id = process_import_table(bq_client, table_id)
    # print('process_csv_in_bulk() output_table_id: %s' % output_table_id)
    
    # Export results to Google Cloud Storage
    # Make this work for big files that get split
    destination_uri = "gs://localityservice/output/{}".format(output_filename)
#    print('process_csv_in_bulk() destination_uri: %s' % destination_uri)
    export_table(bq_client, output_table_id, destination_uri)

#     blob = bucket.blob('output/' + filename)
#     blob.make_public()
#     output_url = blob.public_url
#     print('process_csv() output_url: %s' % output_url)

    # Remove the georefs table from BigQuery
#     try:
#         bq_client.get_table(table_id)
#         delete_table(bq_client, table_id)
#         try:
#             bq_client.get_table(table_id)
#         except Exception as e:
#             print('Table %s was deleted.' % (table_id))
#     except Exception as e:
#         print('Table %s does not exist.' % (table_id))


#     output = create_output(return_list)
#     blob = bucket.blob('output/' + filename)
#     blob.upload_from_string(output, content_type='application/csv')
#     blob.make_public()
#     output_url = blob.public_url
#     print('process_csv() output_url: %s' % output_url)

    # Store that output
#     try:
#         send_email(email, output_url)
#     except Exception as e:
#         print("Error sending email: %s. File stored at %s" % (e, output_url))

def process_csv(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    import base64

    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    if 'data' not in event:
        raise ValueError('no data provided')

    config = base64.b64decode(event['data']).decode('utf-8')
    json_config = json.loads(config)
    json_config = json_config['data']
    file_url = json_config['file_url'] # Google Cloud Storage location of input file
    filename = json_config['filename'] # User-provided Output file name
    email = json_config['email']
    header = json_config['header']

    # Alter output file name to [filename]-[UUID of input file location].csv
    filename = '%s-%s.csv' % (filename, file_url.split('/')[1])

    # Don't allow any of the following characters in output file names, substitute '_'
    filename = re.sub(r'[ ~`!@#$%^&*()_+={\[}\]|\\:;"<,>?\'/]', '_', filename)

    print('process_csv() file_url: %s' % file_url)
    print('process_csv() filename: %s' % filename)
    print('process_csv() header: %s' % header)
    print('process_csv() email: %s' % email)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('localityservice')
    blob = bucket.get_blob(file_url)
    bq_client = bigquery.Client()
    with temp_file() as name:
        blob.download_to_filename(name)
        #blob.delete() # Do not leak documents in storage
        return_list = find_best_georef(bq_client, name)

    output = create_output(return_list)
    blob = bucket.blob('output/' + filename)
    blob.upload_from_string(output, content_type='application/csv')
    blob.make_public()
    output_url = blob.public_url
    print('process_csv() output_url: %s' % output_url)

    # Store that output
    try:
        send_email(email, output_url)
    except Exception as e:
        print("Error sending email: %s. File stored at %s" % (e, output_url))

def find_best_georef(client, filename):
    # vocabpath is the location of vocabulary files to test with
    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, '../vocabularies/')

    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    
    listToCsv = []
    # logging.info('find_best_georef() filename: %s' % filename)
    for row in safe_read_csv_row(filename):
        # logging.info('row: %s' % row)
        rowdict = row_as_dict(row)

        loc = darwinize_dict(row_as_dict(row), darwincloudfile)
        lowerloc = lower_dict_keys(loc)

        sanscoordslocmatchstr = location_match_str(locationmatchsanscoordstermlist, lowerloc)
        matchstr = super_simplify(sanscoordslocmatchstr)

#         s = '%s' % __version__
#         s += ' getting best_georef for: %s' % matchstr
#         print(s)

        result = get_best_sans_coords_georef_reduced(client, matchstr)
        if result:
            for field in ['dwc_location_hash', 'locationid']:
                if field in result:
                    result[field] = base64.b64encode(result[field]).decode('utf-8')
            row.update(result)
#             s = '%s' % __version__
#             s += ' best_georef: %s' % row
#             print(s)
        else:
            # Create a dict of empty results for all results fields anyway to make sure
            # the header is correct even if the first result is empty.
#             s = '%s' % __version__
#             s += ' no georef found for: %s' % matchstr
#             print(s)
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

def send_email(target, output_url):
    import sendgrid
    from sendgrid.helpers.mail import Email, To, Content, Mail

    sg = sendgrid.SendGridAPIClient(SENDGRID_API_KEY)
    from_email = Email("vertnetinfo@vertnet.org")
    to_email = To(target)
    subject = "File ready for download from Biodiversity Enhanced Location Services (BELS)"
    content = Content("text/plain", "A request to process a file uploaded to BELS to find the best existing georeferences and send the results to this email address has been completed. The resulting file can be found at %s. This file will be retained at this location for a period of at least 30 days." % output_url)
    message = Mail(from_email, to_email, subject, content)

    sg.client.mail.send.post(request_body=message.get())

@contextmanager
def temp_file():
    _, name = tempfile.mkstemp()
    try:
        yield name
    finally:
        os.remove(name)
