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

"""Prototype module for publishing data to VertNet."""

import appcfg
appcfg.fix_sys_path()

from google.appengine.api import users

import sys
sys.path = ['../../app'] + sys.path

from app import Publisher, Collection, Record

from ndb import model
from ndb import query

import copy
import csv
import hashlib
import logging
from optparse import OptionParser
import os
import re
import simplejson
import sqlite3
import time
import urllib
from uuid import uuid4
from abc import ABCMeta, abstractmethod, abstractproperty
from google.appengine.tools.appengine_rpc import HttpRpcServer

# CouchDB imports
import couchdb

DB_FILE = 'bulk.sqlite3.db'
CACHE_TABLE = 'cache'
TMP_TABLE = 'tmp'

class Record(object):
    
    @classmethod
    def bulk_payload(cls, recjsons, pkey_urlsafe, ckey_urlsafe):
        """Returns a payload for a batch of records: https://gist.github.com/1108715"""
        return dict(
            publishers=dict(
                key_urlsafe=pkey_urlsafe,
                collections=[dict(
                        key_urlsafe=ckey_urlsafe,
                        records=[cls.entity_payload(recjson) for recjson in recjsons])]))
        
    @classmethod
    def entity_payload(cls, recjson):
        """Returns an entities payload for a single record: https://gist.github.com/1108715"""
        key_urlsafe = recjson['key_urlsafe']
        recjson.pop('key_urlsafe')
        sourceid_column = 'occurrenceid'        
        corpus = set([x.strip().lower() for x in recjson.values()]) 
        corpus.update(
            reduce(lambda x,y: x+y, 
                   map(lambda x: [s.strip().lower() for s in x.split() if s], 
                       recjson.values()))) # adds tokenized values                
        return dict(
                key_urlsafe=key_urlsafe,
                sourceid_column=sourceid_column,
                entity=recjson,
                index=dict(
                    corpus=list(corpus)))
    

class AppEngine(object):

    class RPC(object):
        """Abstract class for remote procedure calls."""

        __metaclass__ = ABCMeta

        @abstractmethod
        def request_path(self):
            """The path to send the request to, eg /api/appversion/create."""
            pass

        @abstractmethod
        def payload(self):
            """The body of the request, or None to send an empty request"""
            pass

        @abstractmethod
        def content_type(self):
            """The Content-Type header to use."""
            pass

        @abstractmethod
        def timeout(self):
            """Timeout in seconds; default None i.e. no timeout. Note: for large
            requests on OS X, the timeout doesn't work right."""
            pass

        @abstractmethod
        def kwargs(self):
            """Any keyword arguments."""
            pass

    def send(self, rpc):
        return self.server.Send(
            rpc.request_path(),
            rpc.payload(),
            rpc.content_type(),
            rpc.timeout(),
            **rpc.kwargs())

    def __init__(self, host, email, passwd):
        """Initializes the server with user credentials and app details."""
        logging.info('Host %s' % host)
        self.server = HttpRpcServer(
            host,
            lambda:(email, passwd),
            None,
            'vert-net',
            debug_data=True,
            secure=False)

class Bulkloader(object):

    class Request(AppEngine.RPC):

        def __init__(self, payload, pkey_urlsafe, ckey_urlsafe):
            self._payload = simplejson.dumps(
                Record.bulk_payload(payload, pkey_urlsafe, ckey_urlsafe))

        def request_path(self):
            return '/api/bulkload'

        def payload(self):
            return self._payload

        def content_type(self):
            return 'application/x-www-form-urlencoded'

        def timeout(self):
            return None
  
        def kwargs(self):  
            return dict()

    def __init__(self, host, email, passwd):
        self.server = AppEngine(host, email, passwd)  

    def load(self, payload, pkey_urlsafe, ckey_urlsafe):
        return self.server.send(
            Bulkloader.Request(payload, pkey_urlsafe, ckey_urlsafe))

def setupdb():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    # Creates the cache table:
    c.execute('create table if not exists ' + CACHE_TABLE +
              '(reckey text, ' +
              'rechash text, ' +
              'recjson text)')
    # Creates the temporary table:
    c.execute('create table if not exists ' + TMP_TABLE +
              '(reckey text, ' +
              'rechash text, ' +
              'recjson text)')
    # Clears all records from the temporary table:
    c.execute('delete from %s' % TMP_TABLE)
    c.close()
    return conn

def tmprecgen(seq):
    for row in seq:
        recguid = row['occurrenceID']

        # Creates record hash:
        cols = row.keys()
        cols.sort()
        fields = [row[x].strip() for x in cols]
        line = reduce(lambda x,y: '%s%s' % (x, y), fields)

        rechash = hashlib.sha224(line).hexdigest()
        recjson = simplejson.dumps(row)

        yield(recguid, rechash, recjson)

class TmpTable(object):
    def __init__(self, conn, options):
        self.conn = conn
        self.options = options
        self.table = 'tmp'
        self.insertsql = 'insert into %s values (?, ?, ?)' % self.table

    def _rowgenerator(self, rows):
        count = 0
        pkey = model.Key('Publisher', self.options.publisher_name)
        ckey = model.Key('Collection', self.options.collection_name, parent=pkey)
        for row in rows:
            count += 1
            try:
                reckey = model.Key('Record', row['occurrenceid'], parent=ckey).urlsafe()
                cols = row.keys()
                cols.sort()
                fields = [row[x].strip() for x in cols]
                line = reduce(lambda x,y: '%s%s' % (x, y), fields)
                rechash = hashlib.sha224(line).hexdigest()
                row['key_urlsafe'] = reckey
                recjson = simplejson.dumps(row)
                yield (reckey, rechash, recjson)
            except Exception as (strerror):
                logging.error('Unable to process row %s - %s' % (count, strerror))

    def _insertchunk(self, rows, cursor):
        logging.info('%s prepared' % self.totalcount)
        cursor.executemany(self.insertsql, self._rowgenerator(rows))
        self.conn.commit()

    def insert(self):
        csvfile = self.options.csvfile
        logging.info('Inserting %s to tmp table.' % csvfile)
        batchsize = int(self.options.batchsize)
        rows = []
        count = 0
        self.totalcount = 0
        chunkcount = 0
        cursor = self.conn.cursor()
        reader = csv.DictReader(open(csvfile, 'r'), skipinitialspace=True)

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

        logging.info('%s rows inserted to tmp table' % self.totalcount)
                    
        
class NewRecords(object):

    def __init__(self, conn, options, bulkloader):
        self.conn = conn
        self.options = options
        self.bulkloader = bulkloader
        self.insertsql = 'insert into %s values (?, ?, ?)' % CACHE_TABLE
        self.deltasql = "SELECT * FROM %s LEFT OUTER JOIN %s USING (reckey) WHERE %s.reckey is null"
        self.totalcount = 0
        self.pkey_urlsafe = Publisher.key_by_name(
            options.publisher_name).urlsafe()
        self.ckey_urlsafe = Collection.key_by_name(
            options.collection_name, options.publisher_name).urlsafe()

    def _insertchunk(self, cursor, recs, entities):
        logging.info('%s inserted' % self.totalcount)
        print self.bulkloader.load(
            entities, 
            self.pkey_urlsafe, 
            self.ckey_urlsafe)
        cursor.executemany(self.insertsql, recs)
        self.conn.commit()

    def execute(self):
        """
        Payload spec: https://gist.github.com/1108715
        """
        logging.info("Checking for new records")

        batchsize = int(self.options.batchsize)
        cursor = self.conn.cursor()
        newrecs = cursor.execute(self.deltasql % (TMP_TABLE, CACHE_TABLE, CACHE_TABLE))
        entities = []
        recs = []
        count = 0
        self.totalcount = 0

        for row in newrecs.fetchall():
            if count >= batchsize:
                self.totalcount += count
                self._insertchunk(cursor, recs, entities)
                count = 0
                recs = []
                entities = []

            count += 1

            reckey = row[0]
            rechash = row[1]
            recjson = row[2]

            entity = simplejson.loads(recjson)
            entities.append(entity)
            recs.append((reckey, rechash, recjson))

        if count > 0:
            self.totalcount += count
            self._insertchunk(cursor, recs, entities)

        self.conn.commit()
        logging.info('INSERT: %s records inserted' % self.totalcount)


class UpdatedRecords(object):

    def __init__(self, conn, couch):
        self.conn = conn
        self.couch = couch
        self.updatedocrevsql = 'update %s set docrev=? where docid=?' % CACHE_TABLE
        self.updatesql = 'update %s set rechash=?, recjson=? where docid=?' % CACHE_TABLE
        self.deltasql = 'SELECT c.recguid, t.rechash, c.recjson, c.docid, c.docrev FROM %s as t, %s as c WHERE t.recguid = c.recguid AND t.rechash <> c.rechash' % (TMP_TABLE, CACHE_TABLE)

    def _updatechunk(self, cursor, recs, docs):
        """Bulk inserts docs to couchdb and updates cache table doc revision."""
        logging.info('%s updated' % self.totalcount)
        cursor.executemany(self.updatesql, recs)
        self.conn.commit()
        updates = [(doc[2], doc[1]) for doc in self.couch.update(docs)]
        cursor.executemany(self.updatedocrevsql, updates)
        self.conn.commit()

    def execute(self, batchsize):
        logging.info("Checking for updated records")

        cursor = self.conn.cursor()
        updatedrecs = cursor.execute(self.deltasql)
        docs = []
        recs = []
        count = 0
        self.totalcount = 0

        for row in updatedrecs.fetchall():
            if count >= batchsize:
                self._updatechunk(cursor, recs, docs)
                self.totalcount += count
                count = 0
                docs = []
                recs = []
            count += 1
            recguid = row[0]
            rechash = row[1] # Note: This is the new hash from tmp table.
            recjson = row[2]
            docid = row[3]
            docrev = row[4]
            doc = simplejson.loads(recjson)
            doc['_id'] = docid
            doc['_rev'] = docrev
            docs.append(doc)
            recs.append((rechash, recjson, docid))

        if count > 0:
            self.totalcount += count
            self._updatechunk(cursor, recs, docs)

        self.conn.commit()

        logging.info('UPDATE: %s records updated' % self.totalcount)

class DeletedRecords(object):
    def __init__(self, conn, couch):
        self.conn = conn
        self.couch = couch
        self.deletesql = 'delete from %s where recguid=?' % CACHE_TABLE
        self.deltasql = 'SELECT * FROM %s LEFT OUTER JOIN %s USING (recguid) WHERE %s.recguid is null' \
            % (CACHE_TABLE, TMP_TABLE, TMP_TABLE)

    def _deletechunk(self, cursor, recs, docs):
        logging.info('%s deleted' % self.totalcount)
        cursor.executemany(self.deletesql, recs)
        self.conn.commit()
        for doc in docs:
            self.couch.delete(doc)

    def execute(self, batchsize):
        logging.info('Checking for deleted records')

        cursor = self.conn.cursor()
        deletes = cursor.execute(self.deltasql)
        count = 0
        self.totalcount = 0
        docs = []
        recs = []

        for row in deletes.fetchall():
            if count >= batchsize:
                self._deletechunk(cursor, recs, docs)
                self.totalcount += count
                count = 0
                docs = []
                recs = []
            count += 1
            recguid = row[0]
            recs.append((recguid,))
            docid = row[3]
            docrev = row[4]
            doc = {'_id': docid, '_rev': docrev}
            docs.append(doc)

        if count > 0:
            self.totalcount += count
            self._deletechunk(cursor, recs, docs)

        self.conn.commit()

        logging.info('DELETE: %s records deleted' % self.totalcount)

def execute(options):
    conn = setupdb()
    batchsize = int(options.batchsize)
    couch = couchdb.Server(options.couchurl)[options.dbname]

    # Loads CSV rows into tmp table:
    TmpTable(conn).insert(options.csvfile, batchsize)

    # Handles new records:
    NewRecords(conn, couch).execute(batchsize)

    # Handles updated records:
    UpdatedRecords(conn, couch).execute(batchsize)

    # Handles deleted records:
    DeletedRecords(conn, couch).execute(batchsize)

    conn.close()


if __name__ == '__main__':

    # Parses command line parameters:
    parser = OptionParser()
    parser.add_option("-f", "--csvfile", dest="csvfile",
                      help="The CSV file",
                      default=None)
    parser.add_option("-d", "--database-name", dest="dbname",
                      help="The CouchDB database name",
                      default=None)
    parser.add_option("-b", "--batch-size", dest="batchsize",
                      help="The chunk size",
                      default=None)
    parser.add_option("-l", "--log-file", dest="logfile",
                      help="A file to save log output to",
                      default=None)
    parser.add_option("-p", "--publisher-name", dest="publisher_name",
                      help="The VertNet publisher ID",
                      default=None)
    parser.add_option("-c", "--collection-name", dest="collection_name",
                      help="The VertNet collection ID",
                      default=None)
    parser.add_option("-u", "--admin-email", dest="admin_email",
                      help="The VertNet admin email",
                      default=None)
    parser.add_option("-w", "--admin-password", dest="admin_password",
                      help="The VertNet admin password",
                      default=None)
    parser.add_option("-t", "--host", dest="host",
                      help="The VertNet App Engine host",
                      default=None)

    (options, args) = parser.parse_args()

#    if options.logfile:
#        logging.basicConfig(level=logging.DEBUG, filename=options.logfile)
#    else:#
    logging.basicConfig(level=logging.DEBUG)

    os.environ['AUTH_DOMAIN'] = 'gmail.com'
    os.environ['USER_EMAIL'] = options.admin_email
    user = users.User(email=options.admin_email)
 
    # publisher = Publisher.create(
    #     options.publisher_name, 
    #     user,
    #     'bulkload',
    #     'vertnet')
    
    # collection = Collection.create(
    #     options.collection_name, 
    #     publisher.key,
    #     user,
    #     'bulkload',
    #     'vertnet')
    
    bulkloader = Bulkloader(options.host, options.admin_email, options.admin_password)
    #print r.load(collection._to_pb().SerializeToString())
    #r.load(dict(aaron="cool"))
    #print r.load([
    #        dict(
    #            publisher_key=publisher.key.urlsafe(), 
    #            collection_key=collection.key.urlsafe())])

    conn = setupdb()
    batchsize = int(options.batchsize)

    # Loads CSV rows into tmp table:
    TmpTable(conn, options).insert()

    # Handles new records:
    NewRecords(conn, options, bulkloader).execute()

    # Handles updated records:
    # UpdatedRecords(conn, couch).execute(batchsize)

    # Handles deleted records:
    # DeletedRecords(conn, couch).execute(batchsize)

    conn.close()
    
    #execute(options)
   
