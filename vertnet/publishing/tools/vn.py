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

"""This module runs VertNet actions and is based on the Google App Engine
appcfg.py design."""

# Standard Python modules
import copy
import csv
import hashlib
import logging
import optparse
import os
import re
import simplejson
import sqlite3
import sys
import time
import urllib
from uuid import uuid4

# VertNet app
from app import Publisher, Collection, Record

# Google App Engine
from google.appengine.api import users

# DatastorePlus
from ndb import model
from ndb import query

def PrintUpdate(msg):
    if verbosity > 0:
        print >>sys.stderr, msg

def StatusUpdate(msg):
    PrintUpdate(msg)

def _ReportOptions(self, parser):
    pass

def _DeltasOptions(self, parser):

    parser.add_option('-b', '--batch_size', type='int', dest='batch_size',
                      default=25000, metavar='SIZE',
                      help='Batch size for processing.')

    parser.add_option('-f', '--csv_file', type='string', dest='csv_file',
                      metavar='FILE', help='Input CSV file.')

    parser.add_option('-p', '--publisher_name', type='string', dest='publisher_name',
                      metavar='NAME', help='VertNet publisher name.')

    parser.add_option('-c', '--collection_name', type='string', dest='collection_name',
                      metavar='NAME', help='VertNet publisher collection name.')


class DeltaProcessor(object):

    DB_FILE = 'bulk.sqlite3.db'
    CACHE_TABLE = 'cache'
    TMP_TABLE = 'tmp'

    class TmpTable(object):

        def __init__(self, conn, options, table):
            self.conn = conn
            self.options = options
            self.table = table
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
                    ErrorUpdate('Unable to process row %s - %s' % (count, strerror))

        def _insertchunk(self, rows, cursor):
            cursor.executemany(self.insertsql, self._rowgenerator(rows))
            self.conn.commit()
            StatusUpdate('%s...' % self.totalcount)

        def insert(self):
            csvfile = self.options.csv_file
            StatusUpdate('Processing incoming records')
            batchsize = int(self.options.batch_size)
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

            StatusUpdate('Processed %s records' % self.totalcount)

    class NewRecords(object):

        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            self.insertsql = 'insert into cache values (?, ?, ?, ?)' 
            self.deltasql = "SELECT * FROM tmp LEFT OUTER JOIN cache USING (reckey) WHERE cache.reckey is null"
            self.totalcount = 0
            reader = csv.DictReader(open(self.options.csv_file, 'r'), skipinitialspace=True)
            columns = [x.lower() for x in reader.next().keys()]            
            columns.append('key_urlsafe')
            self.writer = csv.DictWriter(open('new.csv', 'w'), columns, quoting=csv.QUOTE_ALL)
            self.writer.writeheader()
            self.pkey_urlsafe = Publisher.key_by_name(
                options.publisher_name).urlsafe()
            self.ckey_urlsafe = Collection.key_by_name(
                options.collection_name, options.publisher_name).urlsafe()

        def _insertchunk(self, cursor, recs, entities):
            self.writer.writerows(entities)
            cursor.executemany(self.insertsql, recs)
            self.conn.commit()
            StatusUpdate('%s...' % self.totalcount)

        def execute(self):
            StatusUpdate("Checking for new records")

            batchsize = int(self.options.batch_size)
            cursor = self.conn.cursor()
            newrecs = cursor.execute(self.deltasql)
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
                recs.append((reckey, rechash, recjson, 'new'))
            
            if count > 0:
                self.totalcount += count
                self._insertchunk(cursor, recs, entities)

            self.conn.commit()
            if self.totalcount > 0:
                StatusUpdate('%s new records saved to new.csv' % self.totalcount)
            else:
                os.remove('new.csv')
                StatusUpdate('No new records')



    
    class UpdatedRecords(object):
        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            self.updatesql = 'update cache set rechash=?, recjson=? , recstate=? where reckey=?'
            self.deltasql = 'SELECT c.reckey, t.rechash, t.recjson FROM tmp as t, cache as c WHERE t.reckey = c.reckey AND t.rechash <> c.rechash'        
            reader = csv.DictReader(open(self.options.csv_file, 'r'), skipinitialspace=True)
            columns = [x.lower() for x in reader.next().keys()]            
            columns.append('key_urlsafe')
            self.writer = csv.DictWriter(open('updated.csv', 'w'), columns, quoting=csv.QUOTE_ALL)
            self.writer.writeheader()

        def _updatechunk(self, cursor, recs, entities):
            """Bulk inserts docs to couchdb and updates cache table doc revision."""
            self.writer.writerows(entities)
            cursor.executemany(self.updatesql, recs)
            self.conn.commit()
            StatusUpdate('%s...' % self.totalcount)

        def execute(self):
            StatusUpdate("Checking for updated records")
            batchsize = int(self.options.batch_size)
            cursor = self.conn.cursor()
            updatedrecs = cursor.execute(self.deltasql)
            entities = []
            recs = []
            count = 0
            self.totalcount = 0

            for row in updatedrecs.fetchall():
                if count >= batchsize:
                    self._updatechunk(cursor, recs, entities)
                    self.totalcount += count
                    count = 0
                    entities = []
                    recs = []
                count += 1
                reckey = row[0]
                rechash = row[1] # Note: This is the new hash from tmp table.
                recjson = row[2]

                entity = simplejson.loads(recjson)
                entities.append(entity)
                
                recs.append((rechash, recjson, 'updated', reckey))

            if count > 0:
                self.totalcount += count
                self._updatechunk(cursor, recs, entities)

            self.conn.commit()
            if self.totalcount > 0:
                StatusUpdate('%s updated records saved to updated.csv' % self.totalcount)
            else:
                os.remove('updated.csv')
                StatusUpdate('No updated records')

    class DeletedRecords(object):
        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            self.deletesql = 'delete from cache where reckey=?'
            self.updatesql = 'update cache set recstate=? where reckey=?'
            self.deltasql = 'SELECT * FROM cache LEFT OUTER JOIN tmp USING (reckey) WHERE tmp.reckey is null'
            columns = ['key_urlsafe']
            self.writer = csv.DictWriter(open('deleted.csv', 'w'), columns, quoting=csv.QUOTE_ALL)
            self.writer.writeheader()

        def _deletechunk(self, cursor, recs, entities):
            cursor.executemany(self.updatesql, recs)
            self.conn.commit()
            self.writer.writerows(entities)
            StatusUpdate('%s...' % self.totalcount)

        def execute(self):
            StatusUpdate("Checking for deleted records")
            batchsize = int(self.options.batch_size)
            cursor = self.conn.cursor()
            deletes = cursor.execute(self.deltasql)
            count = 0
            self.totalcount = 0
            entities = []
            recs = []

            for row in deletes.fetchall():
                if count >= batchsize:
                    self._deletechunk(cursor, recs, entities)
                    self.totalcount += count
                    count = 0
                    entities = []
                    recs = []
                count += 1
                reckey = row[0]
                recs.append(('deleted', reckey))
                entities.append(dict(key_urlsafe=reckey))

            if count > 0:
                self.totalcount += count
                self._deletechunk(cursor, recs, entities)

            self.conn.commit()
            if self.totalcount > 0:
                StatusUpdate('%s deleted records saved to deleted.csv' % self.totalcount)
            else:
                os.remove('deleted.csv')
                StatusUpdate('No deleted records')


    class Report(object):
        def __init__(self, conn, options):
            self.conn = conn
            self.options = options
            columns = ['recstate', 'reckey', 'rechash', 'recjson']
            self.writer = csv.DictWriter(open('report.csv', 'w'), columns, quoting=csv.QUOTE_ALL)
            self.writer.writeheader()
        
        def execute(self):
            StatusUpdate('Creating report')
            cursor = self.conn.cursor()            
            for row in cursor.execute('select reckey, rechash, recjson, recstate from cache'):
                self.writer.writerow(dict(
                        reckey=row[0],
                        rechash=row[1],
                        recjson=row[2],
                        recstate=row[3]))
            StatusUpdate('Report saved to report.csv')
                                    

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

class Action(object):
    """Contains information about a command line action."""

    def __init__(self, function, usage, short_desc, long_desc='',
                 error_desc=None, options=lambda obj, parser: None,
                 uses_basepath=True):
        """Initializer for the class attributes."""
        self.function = function
        self.usage = usage
        self.short_desc = short_desc
        self.long_desc = long_desc
        self.error_desc = error_desc
        self.options = options
        self.uses_basepath = uses_basepath

    def __call__(self, appcfg):
        """Invoke this Action on the specified Vn."""
        method = getattr(appcfg, self.function)
        return method()

class Vn(object):

    actions = dict(
        help=Action(
            function='Help',
            usage='%prog help <action>',
            short_desc='Print help for a specific action.',
            uses_basepath=False),
        deltas=Action(
            function='Deltas',
            usage='%prog [options] deltas <file>',
            options=_DeltasOptions,
            short_desc='Calculate deltas for a file.',
            long_desc="""
Specify a CSV file, and vn.py will generate deltas for new, updated
and deleted records. Deltas will be saved to new files (new.csv,
updated.csv, deleted.csv)."""),
        report=Action(
            function='Report',
            usage='%prog [options] report',
            options=_ReportOptions,
            short_desc='Creates a report.',
            long_desc="""Creates a report that details new, updated
and deleted records."""))

    def __init__(self, argv, parser_class=optparse.OptionParser):
        self.parser_class = parser_class
        self.argv = argv

        self.parser = self._GetOptionParser()
        for action in self.actions.itervalues():
            action.options(self, self.parser)

        self.options, self.args = self.parser.parse_args(argv[1:])

        if len(self.args) < 1:
            self._PrintHelpAndExit()

        action = self.args.pop(0)

        if action not in self.actions:
            self.parser.error("Unknown action: '%s'\n%s" %
                              (action, self.parser.get_description()))

        self.action = self.actions[action]


        self.parser, self.options = self._MakeSpecificParser(self.action)

        if self.options.help:
            self._PrintHelpAndExit()

        if self.options.verbose == 2:
            logging.getLogger().setLevel(logging.INFO)
        elif self.options.verbose == 3:
            logging.getLogger().setLevel(logging.DEBUG)

        global verbosity
        verbosity = self.options.verbose


    def Run(self):
        try:
            self.action(self)
        except:
            return 1
        return 0

    def Help(self, action=None):
        """Prints help for a specific action."""
        if not action:
            if len(self.args) > 1:
                self.args = [' '.join(self.args)]

        if len(self.args) != 1 or self.args[0] not in self.actions:
            self.parser.error('Expected a single action argument. '
                              ' Must be one of:\n' +
                              self._GetActionDescriptions())
        action = self.args[0]
        action = self.actions[action]
        self.parser, unused_options = self._MakeSpecificParser(action)
        self._PrintHelpAndExit(exit_code=0)

    def Report(self):
        StatusUpdate('Creating a report')
        DeltaProcessor(self.options).report()

    def Deltas(self):
        csv_file = self.options.csv_file
        if not csv_file:
            logging.critical('CSV required')
            sys.exit(1)
        StatusUpdate('Calculating deltas for %s' % csv_file)
        DeltaProcessor(self.options).deltas()

    def _PrintHelpAndExit(self, exit_code=2):
        """Prints the parser's help message and exits the program."""
        self.parser.print_help()
        sys.exit(exit_code)

    def _GetActionDescriptions(self):
        """Returns a formatted string containing the short_descs for all actions."""
        action_names = self.actions.keys()
        action_names.sort()
        desc = ''
        for action_name in action_names:
            desc += '  %s: %s\n' % (action_name, self.actions[action_name].short_desc)
        return desc

    def _MakeSpecificParser(self, action):
        """Creates a new parser with documentation specific to 'action'."""
        parser = self._GetOptionParser()
        parser.set_usage(action.usage)
        parser.set_description('%s\n%s' % (action.short_desc, action.long_desc))
        action.options(self, parser)
        options, unused_args = parser.parse_args(self.argv[1:])
        return parser, options

    def _GetOptionParser(self):
        """Creates an OptionParser with generic usage and description strings."""

        class Formatter(optparse.IndentedHelpFormatter):
            """Custom help formatter that does not reformat the description."""

            def format_description(self, description):
                """Very simple formatter."""
                return description + '\n'

        desc = self._GetActionDescriptions()
        desc = ('Action must be one of:\n%s'
                'Use \'help <action>\' for a detailed description.') % desc

        parser = self.parser_class(usage='%prog [options] <action>',
                                   description=desc,
                                   formatter=Formatter(),
                                   conflict_handler='resolve')

        parser.add_option('-h', '--help', action='store_true',
                          dest='help', help='Show the help message and exit.')

        parser.add_option('-v', '--verbose', action='store_const', const=2,
                          dest='verbose', default=1,
                          help='Print info level logs.')


        return parser




def main(argv):
    logging.basicConfig(format=('%(asctime)s %(levelname)s %(filename)s:'
                                '%(lineno)s %(message)s '))
    try:
        result = Vn(argv).Run()
        if result:
            sys.exit(result)
    except KeyboardInterrupt:
        #StatusUpdate('Interrupted.')
        sys.exit(1)

if __name__ == '__main__':
    main(sys.argv)
