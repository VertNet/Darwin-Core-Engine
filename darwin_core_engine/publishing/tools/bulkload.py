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
import sqlite3
import subprocess
import simplejson
import sys
import tempfile

# Datastore Plus modules
from ndb import model

# CouchDB modules
import couchdb

class Bulkload(object):
    def __init__(self, options):
        self.options = options
            
    def execute(self):
        logging.info('Bulkloading')

        if self.options.localhost:
            self.options.url = 'http://localhost:8080/_ah/remote_api'
        
        # Bulkload Record
        log_file = tempfile.NamedTemporaryFile(delete=False)  
        cmd = 'appcfg.py upload_data --log_file=%s --batch_size=%s --num_threads=%s --config_file=%s --filename=%s --kind Record --url=%s' % \
            (log_file.name, self.options.batch_size, self.options.num_threads, 
             self.options.config_file, self.options.filename, self.options.url)
        logging.info(cmd)
        args = shlex.split(cmd)
        subprocess.call(args, bufsize=-1)

        # Get progress sqlite3 database name
        log_file.flush()
        log_file.seek(0)
        rec_db_filename = None
        for line in log_file.readlines():
            if line.rfind('Opening database:') != -1:
                rec_db_filename = line.split(':')[3].strip()
        log_file.seek(0)

        # Bulkload RecordIndex
        cmd = 'appcfg.py --log_file=%s upload_data --batch_size=%s --num_threads=%s --config_file=%s --filename=%s --kind RecordIndex --url=%s' % \
           (log_file.name, self.options.batch_size, self.options.num_threads, 
            self.options.config_file, self.options.filename, self.options.url)
        logging.info(cmd)
        args = shlex.split(cmd) 
        subprocess.call(args) 
        
        # Get progress sqlite3 database name
        log_file.flush()
        log_file.seek(0)
        recindex_db_filename = None
        for line in log_file.readlines():
            if line.rfind('Opening database:') != -1:
                recindex_db_filename = line.split(':')[3].strip()

        # Update cache.recstate to published or error
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        values = self._reckeys_not_bulkloaded(rec_db_filename, recindex_db_filename)
        sql = 'update cache set recstate=? where reckey=?'
        recs = cur.executemany(sql, values)
        conn.commit()

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

    
    def _reckeys_not_bulkloaded(self, rec_db_filename, recindex_db_filename):
        """Generator for (reckey, state) where state is published or error."""
        # Get rows in Record progress database
        rec_conn = sqlite3.connect(rec_db_filename, check_same_thread=False)
        rec_cur = rec_conn.cursor()
        recs = rec_cur.execute('select state from progress')

        # Get rows in RecordIndex progress database
        index_conn = sqlite3.connect(recindex_db_filename, check_same_thread=False)
        index_cur = index_conn.cursor()
        indexes = index_cur.execute('select state from progress')
        
        # Get reader for report.csv
        f = codecs.open(self.options.filename, encoding='utf-8', mode='r')
        report = UnicodeDictReader(f, skipinitialspace=True)
                
        # Yield (state, reckey)
        for row in report:
            reckey = row['reckey']
            rec_state = recs.fetchone()[0]
            index_state = indexes.fetchone()[0]
            state = 'published'
            # state == 2 means successful bukload
            if rec_state != 2 and index_state != 2:
                state = 'error'
                logging.warn('Record %s failed to bulkload' % reckey)
            yield (state, reckey)
