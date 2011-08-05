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

"""This module contains transformation functions for the bulkloader."""

import test_util
test_util.fix_sys_path()

from django.utils import simplejson
from google.appengine.api import datastore
from google.appengine.ext.bulkload import transform
from google.appengine.ext.db import Expando
from ndb import model
from ndb import query


def create_record_key():
    def wrapper(value, bulkload_state):
        """Returns a Record key built from value.
        
        Arguments:
            value - urlsafe key string created by model.Key.urlsafe()            
        """
        key = model.Key(urlsafe=value)
        flat = key.flat()
        d = bulkload_state.current_dictionary
        d['pname'] = flat[1]
        d['cname'] = flat[3]
        d['rname'] = flat[5]
        return transform.create_deep_key(
            ('Publisher', 'pname'),
            ('Collection', 'cname'),
            ('Record', 'rname'))(value, bulkload_state)
    return wrapper

def create_record_index_key():
    def wrapper(value, bulkload_state): 
        """Returns a RecordIndex key built from value.

        Arguments:
            value - urlsafe key string created by model.Key.urlsafe()            
        """        
        key = model.Key(urlsafe=value)
        flat = key.flat()
        d = bulkload_state.current_dictionary
        d['pname'] = flat[1]
        d['cname'] = flat[3]
        d['rname'] = flat[5]
        return transform.create_deep_key(
            ('Publisher', 'pname'),
            ('Collection', 'cname'),
            ('Record', 'rname'),
            ('RecordIndex', 'rname'))(value, bulkload_state)
    return wrapper

def ignore_if_deleted(input_dict, instance, bulkload_state_copy):    
    if input_dict['recstate'] == 'deleted':
        return datastore.Entity('Record')
    return instance

def get_corpus_list():
    def wrapper(value, bulkload_state):
        """Returns list of unique words in the entire record.
        
        Arguments:
            value - the JSON encoded record
        """
        d = bulkload_state.current_dictionary
        recjson = simplejson.loads(value)
        d.update(recjson)
        bulkload_state.current_dictionary = d
        corpus = set([x.strip().lower() for x in recjson.values()]) 
        corpus.update(
            reduce(lambda x,y: x+y, 
                   map(lambda x: [s.strip().lower() for s in x.split() if s], 
                       recjson.values()))) # adds tokenized values      
        return list(corpus)
    return wrapper

def add_dynamic_properties(input_dict, instance, bulkload_state_copy):    
    for key,value in input_dict.iteritems():
        try:
            instance[key] = value
        except:
            pass
    instance.pop('rechash')
    instance.pop('reckey')
    recstate = instance.pop('recstate')
    if recstate == 'deleted':
        return datastore.Entity('RecordIndex')
    return instance

if __name__ == '__main__':
    class MicroMock(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    create_record_key()(
        'agFfci4LEglQdWJsaXNoZXIiAXAMCxIKQ29sbGVjdGlvbiIBYwwLEgZSZWNvcmQiATEM',
        MicroMock(current_dictionary=dict(app='test')))
