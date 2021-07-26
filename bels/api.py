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
__version__ = __filename__ + ' ' + "2021-07-26T18:49-03:00"

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

    # An file name must be provided.
    if filename is None or len(filename)==0:
        s = f'A name for the output file must be provided.'
        app.logger.error(s)
        return s, 400  # 400 Bad Request

    # Don't allow any of the following characters in output file names, substitute '_'
    filename = re.sub(r'[ ~`!@#$%^&()+={\[}\]|\\:;"<,>?\'/]', '_', filename)

    # Create a FileStorage object for the input file
    f = request.files['csv']
# with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
#        shutil.copyfileobj(response, out_file)
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

@app.route('/')
def index():
    emailplaceholder = 'Notification email address'
    outputfilenameplaceholder = 'Output file name only (output will be CSV)'
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
<!-- TODO: Add an icon to the title bar> -->
<!--        <link rel="icon" href="icon_path" type="image/icon type"> -->
    </head>

    <h1>Biodiversity Enhanced Location Services (BELS) - Georeference Matcher</h1>
    <p>Upload a comma-separated input file that contains 
       <a href="https://dwc.tdwg.org/terms/#location" target="_blank">location information</a>. Choose an email address to which to send the notification when the results are ready. Choose an output file name. This name will form an identifying part of the results file name, which will be a gzipped CSV file or files with an extension .csv.gz added.</p>
    <form method="post" action="/api/bels_csv" enctype="multipart/form-data">
        <p><input type=file name=csv>
        <p><input type=email name=email placeholder="{emailplaceholder}" size="50">
        <p><input type=text name=filename placeholder="{outputfilenameplaceholder}" size="50">
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
                            <P>VertNet 23 Jul 2021 </P>
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
