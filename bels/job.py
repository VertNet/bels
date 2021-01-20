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
import requests
import csv
import io

from .id_utils import dwc_location_hash
from .dwca_utils import safe_read_csv_row
from .bels_query import get_location_by_id
from google.cloud import bigquery


def confirm_hash_big_query(client, filename):
    # vocabpath is the location of vocabulary files to test with
    vocabpath = '../vocabularies/'
    darwincloudfile = vocabpath + 'darwin_cloud.txt'
    
    listToCsv = []

    for row in safe_read_csv_row(filename):
        location_hash_result = dwc_location_hash(row, darwincloudfile)
        print('location_hash_result', location_hash_result)
        result = get_location_by_id(client, location_hash_result)
        row.update(result)
        listToCsv.append(row)

    return listToCsv


def create_output(occurrences):
    fieldsname = occurrences[0].key()
    output_file = io.BytesIO()

    dict_writer = csv.DictWriter(output_file, fieldsname)
    dict_writer.writeheader()
    dict_writer.writerows(listToCsv)

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
    file_url = json_config['file']
    email = json_config['email']

    f = requests.get(file_url)
    csv_content = f.content
    client = bigquery.Client()

    return_list = confirm_hash_big_query(client, csv_content)

    output = create_output(return_list)
    send_email(email, output)

def send_email(target, file_content):
    import sendgrid

    # TODO: configure API Key
    sg = sendgrid.SendGridClient("YOUR_SENDGRID_API_KEY")
    message = sendgrid.Mail()

    message.add_to(target)

    # TODO: Configure Message Information
    message.set_from("EMAIL")
    message.set_subject("Your location data")
    message.set_html("Find attached your csv with data processed")
    attachment = Attachment()
    attachment.content = file_content
    attachment.type = "text/csv"
    attachment.filename = "output.csv"
    attachment.disposition = "attachment"
    message.add_attachement(attachement)
    sg.send(message)




    
