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

"""This module provides bulkloading support to Google App Engine."""

# VertNet modules
from utils import UnicodeDictReader

# Standard Python modules
import codecs
import csv
import logging
import shlex
import subprocess
import simplejson

# Datastore Plus modules
from ndb import model

# CouchDB modules
import couchdb

class Bulkload(object):
    def __init__(self, options):
        logging.info('Boom!')
        self.options = options
            
    def execute(self):
        logging.info('Bulkloading')

        if self.options.localhost:
            self.options.url = 'http://localhost:8080/_ah/remote_api'
        
        # Bulkload Record
        cmd = 'appcfg.py upload_data --batch_size=%s --num_threads=%s --config_file=%s --filename=%s --kind Record --url=%s' % \
            (self.options.batch_size, self.options.num_threads, 
             self.options.config_file, self.options.filename, self.options.url)
        logging.info(cmd)
        args = shlex.split(cmd)
        subprocess.call(args)            

        # Bulkload RecordIndex
        cmd = 'appcfg.py upload_data --batch_size=%s --num_threads=%s --config_file=%s --filename=%s --kind RecordIndex --url=%s' % \
           (self.options.batch_size, self.options.num_threads, 
            self.options.config_file, self.options.filename, self.options.url)
        logging.info(cmd)
        args = shlex.split(cmd) 
        subprocess.call(args)            

        # Set appid and couchdb based dev_server or production
        if self.options.url.rfind('localhost') != -1:
            logging.info('Bulkloading to localhost')
            appid = 'dev~vert-net'
            db = 'vertnet-dev'
        else:
            logging.info('Bulkloading to production')
            appid = 'vert-net'
            db = 'vertnet-prod'

        # Bulkload coordinates to CouchDB
        server = couchdb.Server('http://eighty.iriscouch.com')
        try:
            couch = server[db]
        except couchdb.http.ResourceNotFound as e:
            server.create(db)
            couch = server[db]
            # TODO create places view
        for batch in self.csv_batch(1000, appid):
            couch.update(batch)
        
    def csv_batch(self, batch_size, appid):
        rows = []
        count = 0
        logging.info('batch_size=%s' % batch_size)
        f = codecs.open(self.options.filename, encoding='utf-8', mode='r')
        for row in UnicodeDictReader(f):            
            if count > batch_size:
                logging.info('yield!')
                yield rows
                rows = []
                count = 0
            count += 1
            rec = simplejson.loads(row['recjson'])
            try:
                lat = rec['decimallatitude']
                lng = rec['decimallongitude']
                # Set appid of key
                reckey = model.Key(urlsafe=row['reckey'])
                reckey = model.Key(flat=reckey.flat(), app=appid)
                rows.append(dict(
                        _id=reckey.urlsafe(),
                        loc=[float(lng), float(lat)]))
            except:
                logging.info('fail')
                pass
        if len(rows) > 0:
            logging.info('returning rows')
            yield rows

