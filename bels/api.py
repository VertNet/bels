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
__version__ = __filename__ + ' ' + "2021-07-20T18:51-03:00"

from flask import Flask, request
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

app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')

topic_name = 'csv_processing'

@app.route('/api/bels_csv', methods=['POST'])
def bels_csv():
    # Create a FileStorage object for the input file
    f = request.files['csv']

    # Specify the email address to which to send the notification
    email = request.form['email']
    
    # Set the root file name for the output
    filename = request.form['filename']

    # Don't allow any of the following characters in output file names, substitute '_'
    filename = re.sub(r'[ ~`!@#$%^&()+={\[}\]|\\:;"<,>?\'/]', '_', filename)

    csv_content = f.read()

    ###
    # Try to get the header from uploaded file and figure out how to pass that to the 
    # job that creates the tables to do bulk georeferencing.
    # The following from 
    # https://stackoverflow.com/questions/33070395/not-able-to-parse-a-csv-file-uploaded-using-flask/64280382#64280382
    if not f:
        return "File not uploaded."

#     stream = io.TextIOWrapper(f.stream._file, "UTF8", newline=None)
#     csv_input = csv.reader(stream)
# 
    first_newline = 2^63-1
    first_return = 2^63-1
    try:
        first_newline = csv_content.index(b'\n')
    except:
        pass
    try:
        first_return = csv_content.index(b'\r')
    except:
        pass

    seekto = None
    if first_newline < first_return:
        seekto = first_newline
    elif first_return < first_newline:
        seekto = first_return
    
    if seekto is None:
        s = 'File has no more than one row, so it is data without a header or a header '
        s += 'without data, in neither circumstance of which I am able to help you.'
        print(s)
        app.logger.error(s)
        return -1

    headerline = csv_content[:csv_content.index(b'\n')]
    fieldnames = headerline.decode("utf-8").split(',')
#     fieldnames = None    
#     with open(csv_content, 'r') as infile:
#         reader = csv.DictReader(infile)
#         fieldnames = reader.fieldnames
#         if fieldnames is None:
#             return "File has no header."
# 
#     stream.seek(0)
    ###
    
    client = storage.Client()
    # TODO: make bucket name configurable?
    bucket = client.get_bucket('localityservice')

    # Google Cloud Storage location for uploaded file
    url = f'jobs/{str(uuid.uuid4())}'
    blob = bucket.blob(url)
    blob.upload_from_string(csv_content)

    # TODO: add PROJECT_ID
    topic_path = publisher.topic_path('localityservice', topic_name)
#    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    message_json = json.dumps({
        'data': {
            'upload_file_url': url, # Google Cloud Storage location of input file
            'email': email,
            'output_filename': filename, # Altered output file name
            'header' : fieldnames, # Header read from uploaded file
        }
    })
    message_bytes = message_json.encode('utf-8')

    logging.info(message_json)
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        # Verify that publish() succeeded
        r = publish_future.result()
        s = f'Success publishing request {message_json}'
        logging.info(s)
        app.logger.info(s)

        blacklist = ["email@example.com", "stuff@things.com"]
        m = []
        if email in blacklist:
           m.append(f'Email is disallowed to destination {email}. ')
           m.append(f'Please try another email address.')
        else:
            m = f'An email with a link to the results will be sent to {email}.'
        return ''.join(m)
    except Exception as e:
        loggin.error(f'Exception print statement: {e}')
        app.logger.error(e)
        return (e, 500)

@app.route('/')
def index():
    emailplaceholder = 'email@example.com'
    outputfilenameplaceholder = 'georefsfoundforme.csv'
    return f'''
<!DOCTYPE html>
<!--
    /*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
    http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
    
    __author__ = "John Wieczorek"
    __copyright__ = {__copyright__}
    __version__ = __version__
    */
    -->
<html>
    <head>
        <meta http-equiv="content-type" content="text/html; charset=UTF-8">
        <LINK href="api.css" rel="stylesheet" type="text/css">
        <TITLE>BELS Georeference Matcher</TITLE>
    </head>

    <form method="post" action="/api/bels_csv" enctype="multipart/form-data">
        <p><input type=file name=csv>
        <p><input type=email name=email placeholder="{emailplaceholder}">
        <p><input type=text name=filename placeholder="{outputfilenameplaceholder}">
        <p><input type=submit value=Submit>
    </form>
        <DIV id="divLinks" style="background-color: #FFFFFF";>
            <HR>
            <TABLE cellspacing="0" border="0" width="100%">
                <TBODY>
                    <TR>
                        <TD width="60%" valign="MIDDLE">
                            <FONT size="2">
                                <P align="LEFT">
                                    {__copyright__}
                            </FONT>
                        </TD>
                        <TD width="40%" valign="MIDDLE" align="RIGHT">
                            <A rel="license" href="http://www.apache.org/licenses/LICENSE-2.0">
                            <img alt="Creative Commons License" style="border-width:0" 
                                src="http://i.creativecommons.org/l/by-sa/3.0/88x31.png"></A>
                            <!--    <img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-sa/3.0/88x31.png">-->
                        </TD>
                    </TR>
                </TBODY>
            </TABLE>
<!--            <iframe width="100%" height="300px" src="https://docs.google.com/spreadsheets/d/e/2PACX-1vTRfHPphVPJhHGMIotzHEUfNUB1PWyBldGp6p2e7deNfYaj4IvKhaKIwm1go5tGmhHdBAc_n5nfX72S/pubhtml?gid=0&amp;single=true&amp;widget=true&amp;headers=false"></iframe>
-->
            <TABLE cellspacing="0" border="0" width="100%">
                <TBODY>
                    <TR>
                        <TD width="50%" valign="MIDDLE">
                            <P>VertNet 12 Jul 2021 </P>
                        </TD>
                        <TD width="50%" valign="MIDDLE" align="RIGHT">
                            <i>Version {__version__}</i>
                        </TD>
                    </TR>
                </TBODY>
            </TABLE>
            <HR>
        </DIV>
    </body>
</html>        
'''

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
