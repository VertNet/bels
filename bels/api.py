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
__version__ = "api.py 2021-03-20T21:29-03:00"

from flask import Flask, request
import bels
import os
import uuid
import datetime
import json
import re

from google.cloud import pubsub_v1
from google.cloud import storage

app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT')

topic_name = 'csv_processing'

@app.route('/api/csv', methods=['POST'])
def csv():

    f = request.files['csv']
    email = request.form['email']
    filename = request.form['filename']
    filename_validation = re.compile('[a-zA-Z0-9_\.-]+')
    if not filename_validation.match(filename):
        return ('invalid filename', 400)
    csv_content = f.read()

    client = storage.Client()
    # TODO: make bucket name configurable?
    bucket = client.get_bucket('localityservice')

    url = 'jobs/%s' % str(uuid.uuid4())
    blob = bucket.blob(url)
    blob.upload_from_string(csv_content)

    # TODO: add PROJECT_ID
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    message_json = json.dumps({
        'data': {
            'file_url': url,# google storage
            'email': email,
            'filename': filename,
        }
    })
    message_bytes = message_json.encode('utf-8')

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        blacklist = ["email@example.com", "stuff@things.com"]
        m = "An email with a link to the results for this request will be sent to %s." % email
        if email in blacklist:
           m = "Email is disallowed to destination %s. Please try another email address." % email
        return m
    except Exception as e:
        print(e)
        return (e, 500)

@app.route('/')
def index():
	return '''
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
	__author__ = "Craig Wieczorek"
	__copyright__ = "Copyright 2022 Rauthiflor LLC"
	__version__ = "gc.html 2021-01-27T13:16-3:00"
	*/
	-->
<html>
	<head>
		<meta http-equiv="content-type" content="text/html; charset=UTF-8">
		<LINK href="gci.css" rel="stylesheet" type="text/css">
		<TITLE>BELS Georeference Matcher</TITLE>
	</head>

    <form method="post" action="/api/csv" enctype="multipart/form-data">
        <p><input type=file name=csv>
        <p><input type=email name=email placeholder="email@example.com">
        <p><input type=text name=filename placeholder="findgeorefsforme.csv">
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
									{0}
							</FONT>
						</TD>
						<TD width="40%" valign="MIDDLE" align="RIGHT">
							<A rel="license" href="http://www.apache.org/licenses/LICENSE-2.0">
							<img alt="Creative Commons License" style="border-width:0" 
								src="http://i.creativecommons.org/l/by-sa/3.0/88x31.png"></A>
							<!--	<img alt="Creative Commons License" style="border-width:0" src="http://i.creativecommons.org/l/by-sa/3.0/88x31.png">-->
						</TD>
					</TR>
				</TBODY>
			</TABLE>
<!--			<iframe width="100%" height="300px" src="https://docs.google.com/spreadsheets/d/e/2PACX-1vTRfHPphVPJhHGMIotzHEUfNUB1PWyBldGp6p2e7deNfYaj4IvKhaKIwm1go5tGmhHdBAc_n5nfX72S/pubhtml?gid=0&amp;single=true&amp;widget=true&amp;headers=false"></iframe>
-->
			<TABLE cellspacing="0" border="0" width="100%">
				<TBODY>
					<TR>
						<TD width="50%" valign="MIDDLE">
							<P>VertNet 20 Mar 2021 </P>
						</TD>
						<TD width="50%" valign="MIDDLE" align="RIGHT">
							<i>Version {1}</i>
						</TD>
					</TR>
				</TBODY>
			</TABLE>
			<HR>
		</DIV>
	</body>
</html>        
    '''.format(__copyright__, __version__)

if __name__ == "__main__":
	app.run(debug=True)

# need an virtualenv to test
# virtualenv --python=python3 env - only the 1st time
# source env/bin/activate
# pip install flask - only the 1st time
# python3 api.py
# curl -v --data-binary @test.csv http://127.0.0.1:5000/api/csv
