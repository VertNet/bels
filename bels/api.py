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
__filename__ = "api.py"
__version__ = __filename__ + ' ' + "2021-10-03T15:25-03:00"

import bels
import os
import uuid
import datetime
import json
import re
import csv
import io
import logging

from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery
from flask import Flask, request, render_template
from flask_restful import Api

from bels.dwca_vocab_utils import darwinize_list
from bels.bels_query import BELS_Client
from bels.resources import BestGeoref

counter = 0
app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
#PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')
PROJECT_ID = 'localityservice'
INPUT_LOCATION = 'bels_input'

topic_name = 'csv_processing'

@app.route('/api/bels_csv', methods=['POST'])
def bels_csv():
    # Retrieve the HTTP POST request parameter value for 'email' from 'request.form' 
    # dictionary.
    # Specifies the email address to which to send the notification
    email = request.form['email']
    
    # An email address must be provided.
    if email is None or len(email)==0:
        s = f'A notification email address must be provided.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    # Do not accept invalid email addresses.
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if re.match(regex, email) == False:
        s = f'{email} is not a valid email address.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request
 
    # Do not accept the following email addresses.
    blacklist = ["email@example.com", "stuff@things.com"]
    m = []
    if email.lower() in blacklist:
        s = f'Email email address {email} is not allowed. '
        s += f'Please try another email address.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    # Set the root file name for the output
    filename = request.form['filename']

    # A file name must be provided.
    if filename is None or len(filename)==0:
        s = f'A name for the output file must be provided.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    # Don't allow any of the following characters in output file names, substitute '_'
    filename = re.sub(r'[ ~`!@#$%^&()+={\[}\]|\\:;"<,>?\'/]', '_', filename)

    # Create a FileStorage object for the input file
    f = request.files['csv']

    # An input file must be provided.
    if f is None:
        s = f'Input file was not uploaded.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    csv_content = f.read()
    
    # The input file can not be empty.
    if len(csv_content) == 0:
        s = f'Input file can not be empty.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    # Try to get the header from uploaded file.
    first_newline = 2**63-1
    first_return = 2**63-1
    try:
        first_newline = csv_content.index(b'\n')
    except:
        pass
    try:
        first_return = csv_content.index(b'\r')
    except:
        pass

    # Find the earlier of \r or \n in the file. This should be the end of the header.
    seekto = None
    if first_newline < first_return:
        seekto = first_newline
    elif first_return < first_newline:
        seekto = first_return

    print(f'first_return={first_return} first_newline={first_newline} seekto={seekto}')
    
    if seekto is None:
        s = 'File has no more than one row, so it is data without a header or a header '
        s += 'without data, in neither circumstance of which I am able to help you.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    # Get the header line and split on commas. Requires the input to be comma-separated.
    headerline = csv_content[:seekto]
    fieldnames = headerline.decode("utf-8").split(',')
    cleaned_fieldnames = []
    for field in fieldnames:
        cleaned_fieldnames.append(field.strip().strip('"').strip("'"))
    app.logger.info(f'headerline: {headerline}')
    app.logger.info(f'cleaned_fieldnames: {cleaned_fieldnames}')
    
    # Darwinize the header
    vocabpath = './bels/vocabularies/'
    dwccloudfile = vocabpath + 'darwin_cloud.txt'
    darwinized_header = darwinize_list(cleaned_fieldnames,dwccloudfile, case='l')
    app.logger.info(f'darwinized_header: {darwinized_header}')
    if 'country' not in darwinized_header and 'countrycode' not in darwinized_header:
        s = 'The uploaded file has no field that can be interpreted as country or '
        s += 'countrycode, at least one of which is required.<br>\n'
        s += f'Darwin Core interpretation of fields found:<br>\n{darwinized_header}'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    client = storage.Client()
    bucket = client.get_bucket(PROJECT_ID)

    # Google Cloud Storage location for uploaded file
    blob_location = f'{INPUT_LOCATION}/{str(uuid.uuid4())}'
    blob = bucket.blob(blob_location)
    blob.upload_from_string(csv_content)
#    url = f'https://storage.cloud.google.com/{PROJECT_ID}/{blob_location}'
    gcs_uri = f'gs://{PROJECT_ID}/{blob_location}'
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    # Create the message to publish.
    message_json = json.dumps({
        'data': {
            'upload_file_url': gcs_uri, # Google Cloud Storage location of input file
            'email': email,
            'output_filename': filename, # Altered output file name
            'header' : cleaned_fieldnames, # Header read from uploaded file
        }
    })
    message_bytes = message_json.encode('utf-8')

    app.logger.info(message_json)

    # Publish the message to the Cloud Pub/Sub topic given by topic_path. This topic is 
    # the trigger for the Cloud functions that subscribe to it. Specifically in this case 
    # csv_processing-1, which gets all the georeferences it can matching the localities
    # in the input file.
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        # Verify that publish() succeeded
        r = publish_future.result()
        s = f'Success publishing request {message_json}'
        logging.info(s)
        app.logger.info(s)

        return f'An email with a link to the results will be sent to {email}.'
    except Exception as e:
        loggin.error(e)
        app.logger.error(e)
        return (e, 500)

bels_client = BELS_Client()
bels_client.populate()
#bels_client.country_report(10)
 
api = Api(app)
api.add_resource(BestGeoref, '/api/bestgeoref', resource_class_kwargs={'bels_client': bels_client})

@app.route('/')
def index(version=None):
    return render_template('index.html', version=__version__)

if __name__ == "__main__":
    app.run(debug=True)

# To test locally...
# need a virtualenv to test
# virtualenv --python=python3 env - only the 1st time
# source env/bin/activate
# pip install flask - only the 1st time
# Make sure the environment value is set for 'GOOGLE_CLOUD_PROJECT'
# If there are changes in job.py, those need to be deployed to be used in api.py locally

# $ python api.py
# open http://127.0.0.1:5000/ in a browser
# Check that the version is as expected
# curl -v --data-binary @test.csv http://127.0.0.1:5000/api/bels_csv

# With Coords bestgeoref examples
#curl -X POST -H "Content-Type: application/json" -d '{"give_me": "BEST_GEOREF", "row": {"continent":"Asia","country":"Philippines", "countrycode":"PH", "locality":"Bacon", "verbatimlocality":"Bacon", "decimallatitude":"13.040245", "decimallongitude":"124.039609"}}' http://127.0.0.1:5000/api/bestgeoref

# Using verbatim coords bestgeoref examples
#curl -X POST -H "Content-Type: application/json" -d '{"give_me": "BEST_GEOREF", "row": {"continent":"Europe", "country":"Portugal", "stateprovince":"Bragança", "municipality":"Bragança", "locality":"Carção", "verbatimlocality":"MC13", "verbatimlatitude":"41.67", "verbatimlongitude":"-6.58"}}' http://127.0.0.1:5000/api/bestgeoref

# Sans Coords bestgeoref examples
#curl -X POST -H "Content-Type: application/json" -d '{"give_me": "BEST_GEOREF", "row": {"continent":"Europe", "country":"United Kingdom", "stateprovince":"England", "county":"Kent County", "locality":"Barnworth"}}' http://127.0.0.1:5000/api/bestgeoref
#curl -X POST -H "Content-Type: application/json" -d '{"give_me": "BEST_GEOREF", "row": {"countrycode": "DK","locality":"Gudhjem"}}' http://127.0.0.1:5000/api/bestgeoref
#curl -X POST -H "Content-Type: application/json" -d '{"give_me": "BEST_GEOREF", "row": {"countrycode": "ES","stateprovince":"Cc", "locality":"Acebo"}}' http://127.0.0.1:5000/api/bestgeoref

# Original record has georef examples
#curl -X POST -H "Content-Type: application/json" -d '{"give_me": "BEST_GEOREF", "row": {"country":"Denmark", "decimallatitude":"20", "decimallongitude":"30", "geodeticdatum":"epsg:4326", "coordinateuncertaintyinmeters":"10", "georeferenceprotocol":"protocol", "georeferencesources":"sources", "georeferenceddate":"date", "georeferencedby":"georefby", "georeferenceremarks":"remarks"}}' http://127.0.0.1:5000/api/bestgeoref
