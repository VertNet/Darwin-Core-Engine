#!/usr/bin/env python
#
# Copyright 2011 Aaron Steele
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""This module provides unittesting coverage for vn.py script."""

# Fixes path for testing:
import test_util
test_util.fix_sys_path()

import logging
import os
import simplejson
import sqlite3
import tempfile
import unittest

from vn import DeltaProcessor

class Options(object):
    """Class that simulates OptParser options object."""
    def __init__(self, opts):
        for item in opts.iteritems():
            self.__dict__[item[0]] = item[1]

class DeltaProcessorTest(unittest.TestCase):
    
    def setUp(self):

        try:
            os.remove('bulk.sqlite3.db')
        except:
            pass

        self.new_data = """occurrenceid,country
1,usa
2,china
3,russia"""
        self.updated_data = """occurrenceid,country
1,united states
2,china
3,russia"""
        self.deleted_data = """occurrenceid,country
3,russia"""
        
    def test_deltas(self):

        # new 
        data_csv = tempfile.NamedTemporaryFile()
        data_csv.write(self.new_data)
        data_csv.flush()
        options = Options(dict(
                publisher_name='p', 
                collection_name='c', 
                batch_size=10000,
                verbosity=1,
                csv_file=data_csv.name))    
        dp = DeltaProcessor(options)
        dp.deltas()
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        for rec in cur.execute('select recstate, recjson from cache'):
            logging.info('new %s' % str(rec))
            self.assertEqual('new', rec[0])
            
        conn.close()

        # updated
        data_csv = tempfile.NamedTemporaryFile()
        data_csv.write(self.updated_data)
        data_csv.flush()
        options = Options(dict(
                publisher_name='p', 
                collection_name='c', 
                batch_size=10000,
                verbosity=1,
                csv_file=data_csv.name))        
        dp = DeltaProcessor(options)
        dp.deltas()
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        for row in cur.execute('select recstate, recjson from cache'):
            logging.info('updated %s' % str(row))
            rec = simplejson.loads(row[1])
            if rec['country'] == 'united states':
                self.assertEqual('updated', row[0])
        conn.close()

        # deleted
        data_csv = tempfile.NamedTemporaryFile()
        data_csv.write(self.deleted_data)
        data_csv.flush()
        options = Options(dict(
                publisher_name='p', 
                collection_name='c', 
                batch_size=10000,
                verbosity=1,
                csv_file=data_csv.name))        
        dp = DeltaProcessor(options)
        dp.deltas()
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        for row in cur.execute('select recstate, recjson from cache'):
            logging.info('deleted %s' % str(row))
            rec = simplejson.loads(row[1])
            if rec['country'] == 'united states' or rec['country'] == 'china':
                self.assertEqual('deleted', row[0])
        conn.close()            

        # new 
        data_csv = tempfile.NamedTemporaryFile()
        data_csv.write(self.new_data)
        data_csv.flush()
        options = Options(dict(
                publisher_name='p', 
                collection_name='c', 
                batch_size=10000,
                verbosity=1,
                csv_file=data_csv.name))    
        dp = DeltaProcessor(options)
        dp.deltas()
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        for row in cur.execute('select recstate, recjson from cache'):
            logging.info('new %s' % str(row))
            rec = simplejson.loads(row[1])
            if rec['country'] == 'usa':
                self.assertEqual('new', row[0])
            if rec['country'] == 'china':
                self.assertEqual('new', row[0])
        conn.close()

        # deleted
        data_csv = tempfile.NamedTemporaryFile()
        data_csv.write(self.deleted_data)
        data_csv.flush()
        options = Options(dict(
                publisher_name='p', 
                collection_name='c', 
                batch_size=10000,
                verbosity=1,
                csv_file=data_csv.name))        
        dp = DeltaProcessor(options)
        dp.deltas()
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        for row in cur.execute('select recstate, recjson from cache'):
            logging.info('deleted %s' % str(row))
            rec = simplejson.loads(row[1])
            if rec['country'] == 'united states' or rec['country'] == 'china':
                self.assertEqual('deleted', row[0])
        conn.close()            

        # new 
        data_csv = tempfile.NamedTemporaryFile()
        data_csv.write(self.new_data)
        data_csv.flush()
        options = Options(dict(
                publisher_name='p', 
                collection_name='c', 
                batch_size=10000,
                verbosity=1,
                csv_file=data_csv.name))    
        dp = DeltaProcessor(options)
        dp.deltas()
        conn = sqlite3.connect('bulk.sqlite3.db', check_same_thread=False)
        cur = conn.cursor()
        for row in cur.execute('select recstate, recjson from cache'):
            logging.info('new %s' % str(row))
            rec = simplejson.loads(row[1])
            if rec['country'] == 'usa':
                self.assertEqual('new', row[0])
            if rec['country'] == 'china':
                self.assertEqual('new', row[0])
        conn.close()

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
