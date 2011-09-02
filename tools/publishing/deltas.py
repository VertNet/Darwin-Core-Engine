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

"""This module provides support for calculating CSV file deltas."""

# VertNet modules
from utils import UnicodeDictReader, UnicodeDictWriter

# Standard Python modules
import codecs
import csv
import hashlib
import logging
import simplejson
import sqlite3
import sys

# Datastore Plus
from ndb import model

class DeltaProcessor(object):

    DB_FILE = 'bulk.sqlite3.db'
    CACHE_TABLE = 'cache'
    TMP_TABLE = 'tmp'

    class TmpTable(object):

        def __init__(self, conn, options, table):
            self.conn = conn
            self.options = options
            self.table = table
            self.insertsql = 'insert into tmp values (?, ?, ?)'
        
        def _rowgenerator(self, rows):
            count = 0
            pkey = model.Key('Publisher', self.options.publisher_name)
            ckey = model.Key('Collection', self.options.collection_name, parent=pkey)            
            source_id = self.options.source_id
            for row in rows:
                count += 1
                try:
                    reckey = model.Key('Record', row[source_id].lower(), parent=ckey).urlsafe()
                    cols = row.keys()
                    cols.sort()
                    fields = [row[x].strip() for x in cols]
                    line = reduce(lambda x,y: '%s%s' % (unicode(x), unicode(y)), fields)
                    rechash = hashlib.sha224(line.encode('utf-8')).hexdigest()
                    recjson = simplejson.dumps(row)
                    yield (reckey, rechash, recjson)
                except Exception as (strerror):
                    logging.error('Unable to process row %s - %s' % (count, strerror))

        def _insertchunk(self, rows, cursor):
            try:
                cursor.executemany(self.insertsql, self._rowgenerator(rows))
                self.conn.commit()
                logging.info('%s...' % self.totalcount)
            except Exception as e:
                logging.error(e)

        def insert(self):
            csvfile = self.options.csv_file
            logging.info('Processing incoming records')
            batchsize = int(self.options.batch_size)
            rows = []
            count = 0
            self.totalcount = 0
            chunkcount = 0
            cursor = self.conn.cursor()
            #f = open(csvfile, 'r')
            f = codecs.open(csvfile, encoding='utf-8', mode='r')
            reader = UnicodeDictReader(f, skipinitialspace=True)
            source_id = self.options.source_id
            if source_id not in [x.lower() for x in reader.fieldnames]:
                logging.critical('The source_id %s is required in csv file' % source_id)
                sys.exit(1)
            for row in reader:
                if count >= batchsize:
                    self.totalcount += count
                    self._insertchunk(rows, cursor)
                    count = 0
                    rows = []
                    chunkcount += 1
                row = dict((k.lower(), v) for k,v in row.iteritems()) # lowercase all keys
                rows.append(row)
                count += 1
            if count > 0:
                self.totalcount += count
                self._insertchunk(rows, cursor)

            logging.info('Processed %s records' % self.totalcount)

    class NewRecords(object):

        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            self.insertsql = 'insert into cache values (?, ?, ?, ?)' 
            self.deltasql = "SELECT * FROM tmp LEFT OUTER JOIN cache USING (reckey) WHERE cache.reckey is null"
            self.deltasql_deleted = "SELECT * FROM tmp LEFT OUTER JOIN cache USING (reckey) WHERE cache.reckey is not null and cache.recstate = 'deleted'"
            self.totalcount = 0
            f = codecs.open(self.options.csv_file, encoding='utf-8', mode='r')
            reader = UnicodeDictReader(f, skipinitialspace=True)
            columns = [x.lower() for x in reader.next().keys()]            

        def _insertchunk(self, cursor, recs):
            cursor.executemany(self.insertsql, recs)
            self.conn.commit()
            logging.info('%s...' % self.totalcount)

        def _insertchunk_update(self, cursor, recs):
            cursor.executemany('update cache set rechash=?, recjson=? , recstate=? where reckey=?', recs)
            self.conn.commit()
            logging.info('%s...' % self.totalcount)

        def execute(self):
            logging.info("Checking for new records")
            batchsize = int(self.options.batch_size)
            cursor = self.conn.cursor()
            newrecs = cursor.execute(self.deltasql)
            recs = []
            count = 0
            self.totalcount = 0

            for row in newrecs.fetchall():
                if count >= batchsize:
                    self.totalcount += count
                    self._insertchunk(cursor, recs)
                    count = 0
                    recs = []
                count += 1
                reckey = row[0]
                rechash = row[1]
                recjson = row[2]
                recs.append((reckey, rechash, recjson, 'new'))
            if count > 0:
                self.totalcount += count
                self._insertchunk(cursor, recs)

            count = 0

            # Handles deleted records in cache table:
            newrecs = cursor.execute(self.deltasql_deleted)
            for row in newrecs.fetchall():
                if count >= batchsize:
                    self.totalcount += count
                    self._insertchunk_update(cursor, recs)
                    count = 0
                    recs = []
                count += 1
                reckey = row[0]
                rechash = row[1]
                recjson = row[2]
                recs.append((rechash, recjson, 'new', reckey))            
            if count > 0:
                self.totalcount += count
                self._insertchunk_update(cursor, recs)

            self.conn.commit()
            if self.totalcount > 0:
                logging.info('%s new records found' % self.totalcount)
            else:
                logging.info('No new records found')
    
    class UpdatedRecords(object):
        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            self.updatesql = 'update cache set rechash=?, recjson=?, recstate=? where reckey=?'
            self.deltasql = 'SELECT c.reckey, t.rechash, t.recjson FROM tmp as t, cache as c WHERE t.reckey = c.reckey AND t.rechash <> c.rechash'        
            f = codecs.open(self.options.csv_file, encoding='utf-8', mode='r')
            reader = UnicodeDictReader(f, skipinitialspace=True)
            columns = [x.lower() for x in reader.next().keys()]            

        def _updatechunk(self, cursor, recs):
            """Bulk inserts docs to couchdb and updates cache table doc revision."""
            cursor.executemany(self.updatesql, recs)
            self.conn.commit()
            logging.info('%s...' % self.totalcount)

        def execute(self):
            logging.info("Checking for updated records")
            batchsize = int(self.options.batch_size)
            cursor = self.conn.cursor()
            updatedrecs = cursor.execute(self.deltasql)
            recs = []
            count = 0
            self.totalcount = 0

            for row in updatedrecs.fetchall():
                if count >= batchsize:
                    self._updatechunk(cursor, recs, entities)
                    self.totalcount += count
                    count = 0
                    recs = []
                count += 1
                reckey = row[0]
                rechash = row[1] # Note: This is the new hash from tmp table.
                recjson = row[2]
                recs.append((rechash, recjson, 'updated', reckey))

            if count > 0:
                self.totalcount += count
                self._updatechunk(cursor, recs)

            self.conn.commit()
            if self.totalcount > 0:
                logging.info('%s updated records found' % self.totalcount)
            else:
                logging.info('No updated records found')

    class DeletedRecords(object):
        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            self.deletesql = 'delete from cache where reckey=?'
            self.updatesql = 'update cache set recstate=? where reckey=?'
            self.deltasql = 'SELECT * FROM cache LEFT OUTER JOIN tmp USING (reckey) WHERE tmp.reckey is null'

        def _deletechunk(self, cursor, recs):
            cursor.executemany(self.updatesql, recs)
            self.conn.commit()
            logging.info('%s...' % self.totalcount)

        def execute(self):
            logging.info("Checking for deleted records")
            batchsize = int(self.options.batch_size)
            cursor = self.conn.cursor()
            deletes = cursor.execute(self.deltasql)
            count = 0
            self.totalcount = 0
            recs = []

            for row in deletes.fetchall():
                if count >= batchsize:
                    self._deletechunk(cursor, recs)
                    self.totalcount += count
                    count = 0
                    recs = []
                count += 1
                reckey = row[0]
                recs.append(('deleted', reckey))

            if count > 0:
                self.totalcount += count
                self._deletechunk(cursor, recs)

            self.conn.commit()
            if self.totalcount > 0:
                logging.info('%s deleted records found' % self.totalcount)
            else:
                logging.info('No deleted records found')

    class Report(object):
        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            columns = ['recstate', 'reckey', 'rechash', 'recjson']
            f = codecs.open('report.csv', encoding='utf-8', mode='w')
            self.writer = UnicodeDictWriter(f, columns, quoting=csv.QUOTE_MINIMAL)
            self.writer.writeheader()
        
        def execute(self):
            logging.info('Creating report')
            cursor = self.conn.cursor()            
            for row in cursor.execute('select reckey, rechash, recjson, recstate from cache'):
                recjson = simplejson.loads(row[2])
                json = simplejson.dumps(dict((k, v) for k,v in recjson.iteritems() if v))
                self.writer.writerow(dict(
                        reckey=row[0],
                        rechash=row[1],
                        recjson=json.encode('utf-8'),
                        recstate=row[3]))
            logging.info('Report saved to report.csv')

    @classmethod
    def setupdb(cls):
        conn = sqlite3.connect(cls.DB_FILE, check_same_thread=False)
        c = conn.cursor()
        # Creates the cache table:
        c.execute('create table if not exists ' + cls.CACHE_TABLE +
                  '(reckey text, ' +
                  'rechash text, ' +
                  'recjson text, ' +
                  'recstate text)')
        # Creates the temporary table:
        c.execute('create table if not exists ' + cls.TMP_TABLE +
                  '(reckey text, ' +
                  'rechash text, ' +
                  'recjson text)')
        # Clears all records from the temporary table:
        c.execute('delete from %s' % cls.TMP_TABLE)
        c.close()
        return conn

    def __init__(self, options):
        self.options = options
        self.conn = DeltaProcessor.setupdb()

    def deltas(self):
        """Calculates deltas and stores in sqlite."""
        self.TmpTable(self.conn, self.options, DeltaProcessor.TMP_TABLE).insert()
        self.NewRecords(self.conn, self.options).execute()
        self.UpdatedRecords(self.conn, self.options).execute()
        self.DeletedRecords(self.conn, self.options).execute()

    def report(self):
        self.Report(self.conn, self.options).execute()

