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
__copyright__ = "Copyright 2021 Rauthiflor LLC"
__version__ = "job.py 2021-01-18"



import base64
import json
import csv
import io
import os
import tempfile

from contextlib import contextmanager

from .id_utils import dwc_location_hash, location_match_str, super_simplify
from .dwca_utils import safe_read_csv_row, lower_dict_keys
from .bels_query import get_location_by_hashid, row_as_dict, get_best_sans_coords_georef_reduced
from .dwca_vocab_utils import darwinize_dict
from .dwca_terms import gbiflocationmatchsanscoordstermlist
from google.cloud import bigquery
from google.cloud import storage


def confirm_hash_big_query(client, filename):
    # vocabpath is the location of vocabulary files to test with
    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, '../vocabularies/')

    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    
    listToCsv = []
    for row in safe_read_csv_row(filename):
        rowdict = row_as_dict(row)

        loc = darwinize_dict(row_as_dict(row), darwincloudfile)
        lowerloc = lower_dict_keys(loc)

        locmatchstr = location_match_str(gbiflocationmatchsanscoordstermlist, lowerloc)
        matchstr = super_simplify(locmatchstr)
        result = get_best_sans_coords_georef_reduced(client, matchstr)
        if result:
            for field in ['dwc_location_hash', 'locationid']:
                if field in result:
                    result[field] = base64.b64encode(result[field]).decode('utf-8')
            row.update(result)

        listToCsv.append(row)

    return listToCsv


def create_output(occurrences):
    fieldsname = occurrences[0].keys()
    output_file = io.StringIO()

    dict_writer = csv.DictWriter(output_file, fieldsname)
    dict_writer.writeheader()
    dict_writer.writerows(occurrences)

    return output_file.getvalue()


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
    file_url = json_config['file_url']
    email = json_config['email']

    client = storage.Client()
    bucket = client.get_bucket('localityservice')
    blob = bucket.get_blob(file_url)
    with temp_file() as name:
        blob.download_to_filename(name)
        #blob.delete() # Do not leak documents in storage
        client = bigquery.Client()

        return_list = confirm_hash_big_query(client, name)

    output = create_output(return_list)
    blob = bucket.blob('output/' + file_url)
    blob.upload_from_string(output, content_type='application/csv')
    blob.make_public()
    output_url = blob.public_url
    print(output_url)

    # Store that output
    send_email(email, output_url)


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
    
