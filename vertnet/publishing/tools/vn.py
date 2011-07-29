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

import logging
import optparse
import sys

import google
import ndb

def _DeltasOptions(self, parser):
    parser.add_option('-b', '--batch_size', type='int', dest='batch_size',
                      default=25000, metavar='SIZE',
                      help='Batch size for processing.')

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
updated.csv, deleted.csv)."""))

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
            logging.info('hi')
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
