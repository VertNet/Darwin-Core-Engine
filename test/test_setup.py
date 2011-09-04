#!/usr/bin/env python

# Copyright 2011 The Regents of the University of California 
#
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

__author__ = "Aaron Steele (eightysteele@gmail.com)"
__copyright__ = "Copyright 2011 The Regents of the University of California"
__contributors__ = ["John Wieczorek (gtuco.btuco@gmail.com)"]

"""This module contains transformation functions for the bulkloader."""

# Setup sys.path for bulkloading
import os, sys
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
DIR_PATH = reduce(lambda x,y: '%s%s%s' % (x,os.path.sep,y), DIR_PATH.split(os.path.sep)[:-1])
EXTRA_PATHS = [
  DIR_PATH,
  os.path.join(DIR_PATH, 'lib', 'google_appengine'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'antlr3'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'django_0_96'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'fancy_urllib'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'ipaddr'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'protorpc'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'webob'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'yaml', 'lib'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'simplejson'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'graphy'),
  os.path.join(DIR_PATH, 'lib', 'appengine-ndb-experiment'),
]

sys.path = EXTRA_PATHS + sys.path

