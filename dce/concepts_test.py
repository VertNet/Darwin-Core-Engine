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
__contributors__ = []

import concepts

import unittest
import logging

class UnitTest(unittest.TestCase):

    def test_transform(self):
        for name, t in concepts.NAME_TYPES.iteritems():
            value = "1.0"
            tvalue = concepts.transform(name, value)
            if t == str:
                self.assertEqual(value, concepts.transform(name, value))
            elif t == float:
                self.assertEqual(float(value), concepts.transform(name, value))
            elif t == int:
                self.assertEqual(int(float(value)), concepts.transform(name, value))
        
    def test_get_full_name(self):
        for name in concepts.FULL_TO_SHORT_NAMES.keys():
            n = concepts.get_full_name(name)
            self.assertEqual(n, name)

    def test_get_short_name(self):
        for short_name in concepts.SHORT_TO_FULL_NAMES.keys():
            sn = concepts.get_short_name(short_name)
            self.assertEqual(sn, short_name)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
