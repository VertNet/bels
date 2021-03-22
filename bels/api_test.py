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
__version__ = "api_test.py 2021-03-21T16:51-03:00"

from bels.job import process_csv
from collections import namedtuple
import base64
import json

event = {
  'data': {
    'file_url': 'jobs/c16cf413-b8d5-4ac3-b200-f27a327bc51b',
    'email': 'gtuco.btuco@gmail.com',
    'filename': 'test_jrw_from_local'
  }
}
event = base64.b64encode(json.dumps(event).encode('utf-8'))
event = {'data':event}
Context = namedtuple('Context', 'event_id timestamp')
context = Context('1234', '1234')

process_csv(event, context)
