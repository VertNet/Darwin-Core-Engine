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
import common
import logging

STOP_WORDS = [
    'a', 'able', 'about', 'across', 'after', 'all', 'almost', 'also', 'am', 
    'among', 'an', 'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 
    'but', 'by', 'can', 'cannot', 'could', 'dear', 'did', 'do', 'does', 'either', 
    'else', 'ever', 'every', 'for', 'from', 'get', 'got', 'had', 'has', 'have', 
    'he', 'her', 'hers', 'him', 'his', 'how', 'however', 'i', 'if', 'in', 'into', 
    'is', 'it', 'its', 'just', 'least', 'let', 'like', 'likely', 'may', 'me', 
    'might', 'most', 'must', 'my', 'neither', 'no', 'nor', 'not', 'of', 'off', 
    'often', 'on', 'only', 'or', 'other', 'our', 'own', 'rather', 'said', 'say', 
    'says', 'she', 'should', 'since', 'so', 'some', 'than', 'that', 'the', 'their', 
    'them', 'then', 'there', 'these', 'they', 'this', 'tis', 'to', 'too', 'twas', 
    'us', 'wants', 'was', 'we', 'were', 'what', 'when', 'where', 'which', 'while', 
    'who', 'whom', 'why', 'will', 'with', 'would', 'yet', 'you', 'your']

# Darwin Core concept names whose value should not be full text indexed
DO_NOT_FULL_TEXT = [
    'acceptednameusageid', 'accessrights', 'basisofrecord', 'collectionid', 
    'coordinateprecision', 'coordinateuncertaintyinmeters', 'datasetid', 
    'dateidentified', 'day', 'decimallatitude', 'decimallongitude', 'disposition', 
    'enddayofyear', 'eventdate', 'eventid', 'eventtime', 'fieldnotes', 
    'footprintspatialfit', 'footprintsrs', 'footprintwkt', 'geologicalcontextid', 
    'georeferenceremarks', 'georeferenceverificationstatus', 'highergeographyid', 
    'identificationid', 'individualcount', 'individualid', 'institutionid', 
    'language', 'locationid', 'maximumdepthinmeters', 
    'maximumdistanceabovesurfaceinmeters', 'maximumelevationinmeters', 
    'minimumdepthinmeters', 'minimumdistanceabovesurfaceinmeters', 
    'minimumelevationinmeters', 'modified', 'month', 'nameaccordingtoid', 
    'namepublishedinid', 'nomenclaturalcode', 'occurrencedetails', 'occurrenceid', 
    'originalnameusageid', 'parentnameusageid', 'pointradiusspatialfit', 'rights', 
    'rightsholder', 'scientificnameid', 'startdayofyear', 'taxonconceptid', 'taxonid', 
    'type', 'verbatimcoordinates', 'verbatimeventdate', 'verbatimlatitude', 
    'verbatimlongitude', 'year']

# Darwin Core concept names that should not be indexed
DO_NOT_INDEX = [
    'acceptednameusageid', 'accessrights', 'associatedmedia', 
    'associatedoccurrences', 'associatedreferences', 
    'associatedsequences', 'associatedtaxa', 'bibliographiccitation', 
    'collectionid', 'datageneralizations', 'datasetid', 'dateidentified', 
    'disposition', 'eventdate', 'eventid', 'eventremarks', 'eventtime', 
    'fieldnotes', 'footprintspatialfit', 'footprintsrs', 'footprintwkt', 
    'geologicalcontextid', 'georeferenceremarks', 'georeferencesources', 
    'habitat', 'higherclassification', 'highergeography', 'highergeographyid', 
    'identificationid', 'identificationreferences', 'identificationremarks', 
    'individualcount', 'individualid', 'informationwithheld', 'institutionid', 
    'locationid', 'locationremarks', 'modified', 'nameaccordingtoid', 
    'namepublishedin', 'namepublishedinid', 'occurrencedetails', #'occurrenceid', 
    'occurrenceremarks', 'originalnameusageid', 'othercatalognumbers', 
    'parentnameusageid', 'pointradiusspatialfit', 'preparations', 
    'previousidentifications', 'rights', 'rightsholder', 'scientificnameid', 
    'taxonconceptid', 'taxonid', 'taxonremarks', 'verbatimcoordinates', 
    'verbatimlatitude', 'verbatimlongitude']

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
        d['rname'] = flat[5].lower()
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
        d['rname'] = flat[5].lower()
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
        recjson = simplejson.loads(value)
        corpus = set(
            [x.strip().lower() for key,x in recjson.iteritems() \
                 if key.strip().lower() not in DO_NOT_FULL_TEXT and \
                 x.strip().lower() not in STOP_WORDS]) 
        corpus.update(
            reduce(lambda x,y: x+y, 
                   map(lambda x: [s.strip().lower() for s in x.split() if s], 
                       [val for key,val in recjson.iteritems() \
                            if key.strip().lower() not in DO_NOT_FULL_TEXT and \
                            val.strip().lower() not in STOP_WORDS]))) # adds tokenized values      
        return list(corpus)
    return wrapper

def add_dynamic_properties(input_dict, instance, bulkload_state_copy):    
    """Adds dynamic properties from the CSV input_dict to the entity instance."""
    recjson = simplejson.loads(input_dict['recjson'])    
    for key,value in recjson.iteritems():
        if key in DO_NOT_INDEX or value.strip() == '':
            continue
        key_name = None

        # Set Darwin Core dynamic property name to the alias
        if key in common.DWC_TO_ALIAS.keys():
            key_name = common.DWC_TO_ALIAS[key]
        else:
            if key in common.ALIAS_TO_DWC.keys():
                key_name = key
        if key_name is None:
            #logging.info('Skipping unknown column %s=%s' % (key, value))
            continue
        try:
            instance[key_name] = value.lower()
        except:
            pass

    # Do not bulkload CSV records that have recstate equal to 'deleted'
    recstate = input_dict['recstate'] #instance.pop('recstate')
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
