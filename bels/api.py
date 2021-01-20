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
__version__ = "api.py 2021-01-18"


from flask import Flask, request
import bels
import uuid

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
    csv_content = f.read()

    client = storage.Client()
    # TODO: make bucket name configurable?
    bucket = client.get_bucket('localityservice')


    blob = bucket.get_blob('jobs/%s' % str(uuid.uuid4())
    blob.upload_from_string(csv_content)
    url = blob.generate_signed_url(expiration=None, version='v4')

    # TODO: add PROJECT_ID
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    message_json = json.dumps({
        'data': {
            'file_url': url,# google storage
            'email': email
        }
    })
    message_bytes = message_json.encode('utf-8')

    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)


@app.route('/')
def index():
	return '''
        <form method="post" action="/api/csv" enctype="multipart/form-data">
            <p><input type=file name=csv>
            <p><input type=email name=email>
            <p><input type=submit value=Login>
        </form>
    '''


if __name__ == "__main__":
	app.run(debug=True)

# need an virtualenv to test
# virtualenv --python=python3 env - only the 1st time
# source env/bin/activate
# pip install flask - only the 1st time
# python3 api.py
# curl -v --data-binary @test.csv http://127.0.0.1:5000/api/csv
