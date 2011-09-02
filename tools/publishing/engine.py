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

# Hack for testing
global verbosity
verbosity = 1

# VertNet modules
from utils import *
from deltas import DeltaProcessor
from bulkload import Bulkload

# Standard Python modules
import copy
import codecs
import logging
import optparse
import os
import re
import shlex
import simplejson
import subprocess
import sys
import urllib

# DatastorePlus
from ndb import model

def PrintUpdate(msg):
    if verbosity > 0:
        print >>sys.stderr, msg

def StatusUpdate(msg):
    PrintUpdate(msg)

def ErrorUpdate(msg):
    PrintUpdate('ERROR: %s' % msg)

def _BulkloadOptions(self, parser):
   parser.add_option('--config_file', type='string', dest='config_file',
                     metavar='FILE', help='Bulkload YAML config file.')
   parser.add_option('--filename', type='string', dest='filename',
                     metavar='FILE', help='CSV file with data to bulkload.')                      
   parser.add_option('--url', type='string', dest='url',
                     help='URL endpoint to /remote_api to bulkload to.')                          
   parser.add_option('--num_threads', type='int', dest='num_threads', default=5,
                     help='Number of threads to transfer records with.')                          
   parser.add_option('--batch_size', type='int', dest='batch_size', default=1,
                     help='Number of records to pst in each request.')                          
   parser.add_option('-l', '--localhost', dest='localhost', action='store_true', 
                      help='Shortcut for bulkloading to http://localhost:8080/_ah/remote_api')                          

def _ReportOptions(self, parser):
    pass

def _DeltasOptions(self, parser):
    parser.add_option('-b', '--batch_size', type='int', dest='batch_size',
                      default=10000, metavar='SIZE',
                      help='Batch size for processing.')
    parser.add_option('-f', '--csv_file', type='string', dest='csv_file',
                      metavar='FILE', help='Input CSV file.')
    parser.add_option('-p', '--publisher_name', type='string', dest='publisher_name',
                      metavar='PUBLISHER', help='VertNet publisher name.')
    parser.add_option('-c', '--collection_name', type='string', dest='collection_name',
                      metavar='COLLECTION', help='VertNet publisher collection name.')
    parser.add_option('-s', '--source_id', type='string', dest='source_id',
                      metavar='SOURCEID', help='Column name that contains the source record id.')

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
and deleted records. Deltas will be persisted via sqlite3."""),
        bulkload=Action( 
            function='Bulkload',
            usage='%prog [options] bulkload <file>',
            options=_BulkloadOptions,
            short_desc='Bulkloads report.csv to Google App Engine datastore.',
            long_desc="""
Specify a CSV file and a URL, and vn.py will bulkload it to the 
Google App Engine datastore."""),
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
        verbosity = self.options.verbose

    def Run(self):
        try:
            self.action(self)
        except Exception as e:
            raise e
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

    def Bulkload(self):
        StatusUpdate('Starting bulkload')
        Bulkload(self.options).execute() 
        StatusUpdate('Bulkloading complete')       

    def Report(self):
        StatusUpdate('Creating a report')
        DeltaProcessor(self.options).report()
        StatusUpdate('Report created')

    def Deltas(self):
        csv_file = self.options.csv_file
        if not csv_file:
            logging.critical('CSV required')
            sys.exit(1)
        StatusUpdate('Calculating deltas for %s' % csv_file)
        DeltaProcessor(self.options).deltas()
        StatusUpdate('Delta calculation complete')

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
        StatusUpdate('Interrupted.')
        sys.exit(1)
    except Exception as e:
        logging.info(e)

if __name__ == '__main__':
    main(sys.argv)
