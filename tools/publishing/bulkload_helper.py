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

"""This module contains transformation functions for the bulkloader."""

# Setup sys.path for bulkloading
import os, sys
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
DIR_PATH = reduce(lambda x,y: '%s%s%s' % (x,os.path.sep,y), DIR_PATH.split(os.path.sep)[:-2])
SCRIPT_DIR = os.path.join(DIR_PATH, 'tools', 'publishing')
EXTRA_PATHS = [
  DIR_PATH,
  SCRIPT_DIR,
  os.path.join(DIR_PATH, 'lib', 'google_appengine'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'antlr3'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'django_0_96'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'fancy_urllib'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'ipaddr'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'protorpc'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'webob'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'yaml', 'lib'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'simplejson'),
  os.path.join(DIR_PATH, 'lib', 'google_appengine', 'lib', 'graphy'),
  os.path.join(DIR_PATH, 'lib', 'appengine-ndb-experiment'),
]
sys.path = EXTRA_PATHS + sys.path

# DCE modules
from dce import concepts

# Standard Python modules
import simplejson
import logging

# App Engine modules
from google.appengine.ext import db
from google.appengine.api import datastore
from google.appengine.ext.bulkload import transform

# NDB modules
from ndb import model

# Words not included in full text search
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

# Darwin Core names not indexed in full text
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

# Darwin Core names not indexed
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
                            val.strip().lower() not in STOP_WORDS]), [])) # adds tokenized values      
        if len(corpus) == 0:
            return None
        return list(corpus)
    return wrapper

def ignore_if_deleted(input_dict, instance, bulkload_state_copy):    
    if input_dict['recstate'] == 'deleted':
        return datastore.Entity('Record')
    return instance

def get_rec_json():
    """Returns a JSON object where all keys have values."""
    def wrapper(recjson, bulkload_state):    
        recjson = simplejson.loads(recjson)
        rec = {}
        for name,value in recjson.iteritems():
            if not value:
                continue
            rec[name] = value
        return db.Text(simplejson.dumps(rec))
    return wrapper

def add_dynamic_properties(input_dict, instance, bulkload_state_copy):    
    """Adds dynamic properties from the CSV input_dict to the entity instance."""

    # Ingore deleted records
    if input_dict['recstate'] == 'deleted':
        return datastore.Entity('RecordIndex')
    
    # Populate dynamic properties using Darwin Core short names
    recjson = simplejson.loads(input_dict['recjson'].encode('utf-8'))
    for name,value in recjson.iteritems():
        full_name = concepts.get_full_name(name)
        if not full_name:
            continue
        value = str(value).strip().lower()
        if full_name in DO_NOT_INDEX or not value:
            continue
        instance[concepts.get_short_name(full_name)] = value
    return instance
