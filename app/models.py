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

# Standard Python imports
import csv
import logging
import os
import simplejson

# Google App Engine imports
from google.appengine.ext import db
from google.appengine.ext import deferred
from google.appengine.ext import webapp
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import users
from google.appengine.api import taskqueue
from google.appengine.ext.webapp.util import login_required
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import entity_pb

# Datastore Plus imports
from ndb import model
from ndb import query

try:
    appid = os.environ['APPLICATION_ID']
    appver = os.environ['CURRENT_VERSION_ID'].split('.')[0]
except:
    pass

# ------------------------------------------------------------------------------
# Models

class BaseModel(model.Model):
    @classmethod
    def get_urlname(cls, name):
        """Returns a URL friendly name by replacing space with '-' char."""
        return '-'.join(''.join(ch for ch in word if ch.isalnum()) \
                            for word in name.split())
    
class Publisher(BaseModel): # key_name=urlname
    """Model for a VertNet data Publisher."""
    name = model.StringProperty('n', required=True)
    urlname = model.ComputedProperty(lambda self: urlname(self.name))
    admins = model.UserProperty('a', repeated=True)
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)
    json = model.TextProperty('j', required=True) # JSON representation

    @classmethod
    def create(cls, name, user, appver, appid):
        """Creates a new Publisher instance."""
        return Publisher(
            id=urlname(name),
            name=name,
            json=simplejson.dumps(dict(
                    url='http://%s.%s.appspot.com/publishers/%s' % \
                        (appver, appid, urlname(name)),
                    name=name,
                    admin=user.nickname())))     
    
    @classmethod
    def get_by_name(cls, name):
        return cls.get_by_urlname(cls.get_urlname(name))

    @classmethod
    def key_by_name(cls, name):
        return model.Key('Publisher', cls.get_urlname(name))

    @classmethod
    def get_by_urlname(cls, urlname):
        """Queries the Publisher model by Publisher.urlname value."""
        return cls.key_by_name.get()


class Collection(BaseModel): # key_name=urlname, parent=Publisher
    """Model for a collection of records."""
    name = model.StringProperty('n', required=True)
    urlname = model.ComputedProperty(lambda self: urlname(self.name))
    admins = model.UserProperty('a', repeated=True)
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)
    json = model.TextProperty('j', required=True) # JSON representation

    @classmethod 
    def isadmin(cls):
        pass

    @classmethod
    def key_by_name(cls, name, publisher_name):
        return model.Key(
            'Collection', 
            cls.get_urlname(name), 
            parent=Publisher.key_by_name(publisher_name))
        
    @classmethod
    def create(cls, name, publisher_key, admin, appver, appid):
        """Creates a new Collection instance."""
        return Collection(
            parent=publisher_key,
            id=urlname(name),
            name=name,
            json=simplejson.dumps(dict(
                    name=name,
                    admin=users.get_current_user().nickname(),
                    url='http://%s.%s.appspot.com/publishers/%s/%s' % \
                        (appver, appid, publisher_key.id(), urlname(name))))) 
    
    @classmethod
    def get_by_name(cls, name, publisher_key):
        return cls.get_by_urlname(cls.get_urlname(name), publisher_key)

    @classmethod
    def get_by_urlname(cls, urlname, publisher_key):
        """Queries the Collection model by Collection.urlname value."""
        return model.Key('Collection', urlname, parent=publisher_key).get()

    @classmethod
    def all_by_publisher(cls, publisher_key):
        """Returns all Collection entities for the given Publisher key."""
        return Collection.query(ancestor=publisher_key).fetch()
    
class Record(BaseModel): # key_name=record.occurrenceid, parent=Collection
    """Model for a record."""
    json = model.TextProperty('r', required=True) # darwin core json representation
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)

    @classmethod
    def create(cls, config):
        collection_key = config.get('collection_key')
        if not collection_key:
            pname = config.get('publisher_name')
            cname = config.get('collection_name')
            publisher = Publisher.get_by_name(pname)
            collection_key = Collection.get_by_name(cname, publisher.key).key            
        return Record(
            parent=collection_key,
            id=rec['occurrenceid'],
            record=simplejson.dumps(rec))

    @classmethod
    def all_by_collection(cls, collection_key):
        """Returns all Record entities for a Collection."""
        # TODO: Should probably just return the query here.
        return Record.query(ancestor=collection_key).fetch()
    
class RecordIndex(model.Expando): # parent=Record
    """Index relation for Record."""
    corpus = model.StringProperty(repeated=True) # full text

    @classmethod
    def create(cls, rec, collection_key):
        """Creates a new RecordIndex instance."""
        key = model.Key(
            'RecordIndex', 
            rec['occurrenceid'], 
            parent=model.Key('Record', rec['occurrenceid'], parent=collection_key))
        index = RecordIndex(key=key, corpus=cls.getcorpus(rec))
        for concept,value in rec.iteritems():
            index.__setattr__(concept, value.lower())
        return index

    @classmethod
    def search(cls, limit, offset, args={}, keywords=[]):
        """Returns all Record entities for the given arguments and keywords.

        Arguments
            args - Dictionary with Darwin Core concept keys
            keywords - list of keywords to search on
        """        
        qry = RecordIndex.query()
        if len(args) > 0:
            gql = 'SELECT * FROM RecordIndex WHERE'
            for k,v in args.iteritems():
                gql = "%s %s='%s' AND " % (gql, k, v)
            gql = gql[:-5] # Removes trailing AND
            qry = query.parse_gql(gql)[0]
        for keyword in keywords:
            qry = qry.filter(RecordIndex.corpus == keyword)        
        logging.info('QUERY='+str(qry))
        return model.get_multi([x.parent() for x in qry.fetch(limit, offset=offset, keys_only=True)])

    @classmethod
    def getcorpus(cls, rec):
        """Returns the full text of the record dictionary."""
        # verbatim values lower case
        corpus = set([x.strip().lower() for x in rec.values()]) 
        corpus.update(
            reduce(lambda x,y: x+y, 
                   map(lambda x: [s.strip().lower() for s in x.split() if s], 
                       rec.values()))) # adds tokenized values
        return list(corpus)
    
