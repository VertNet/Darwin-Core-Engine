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

def urlname(name):
    """Returns a URL friendly name by replacing space with '-' char."""
    return '-'.join(''.join(ch for ch in word if ch.isalnum()) \
                        for word in name.split())

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
    def get_by_urlname(cls, urlname):
        """Queries the Publisher model by Publisher.urlname value."""
        return model.Key('Publisher', urlname).get()


class Collection(BaseModel): # key_name=urlname, parent=Publisher
    """Model for a collection of records."""
    name = model.StringProperty('n', required=True)
    urlname = model.ComputedProperty(lambda self: urlname(self.name))
    admins = model.UserProperty('a', repeated=True)
    created = model.DateTimeProperty('c', auto_now_add=True)
    updated = model.DateTimeProperty('u', auto_now=True)
    json = model.TextProperty('j', required=True) # JSON representation

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
    record = model.TextProperty('r', required=True) # darwin core json representation
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
            id=rec['sourceid'],
            record=simplejson.dumps(rec))

    # @classmethod
    # def create(cls, rec, collection_key):
    #     """Creates a new Record instance."""
    #     return Record(            
    #         parent=collection_key,
    #         id=rec['occurrenceid'],
    #         owner=users.get_current_user(),
    #         record=simplejson.dumps(rec))

    @classmethod
    def all_by_collection(cls, collection_key):
        """Returns all Record entities for a Collection."""
        # TODO: Should probably just return the query here.
        return Record.query(ancestor=collection_key).fetch()
    
class RecordIndex(model.Expando): # parent=Record
    """Index relation for Record."""
    corpus = model.StringProperty('c', repeated=True) # full text

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
    def search(cls, args={}, keywords=[]):
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
        return model.get_multi([x.parent() for x in qry.fetch(keys_only=True)])

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
    
# ------------------------------------------------------------------------------
# Handlers

class BaseHandler(webapp.RequestHandler):
    """Base handler for handling common stuff like template rendering."""
    def render_template(self, file, template_args):
        path = os.path.join(os.path.dirname(__file__), "templates", file)
        self.response.out.write(template.render(path, template_args))
    def push_html(self, file):
        path = os.path.join(os.path.dirname(__file__), "html", file)
        self.response.out.write(open(path, 'r').read())

class UploadForm(BaseHandler):
    @login_required
    def get(self):
        """Returns an upload form HTML page."""
        self.render_template(
            'upload.html', 
            dict(form_url=blobstore.create_upload_url('/upload')))

class FileUploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        """Handler for blobstore uploads."""
        blob_info = self.get_uploads()[0]
        if not users.get_current_user():
            blob_info.delete()
            self.redirect(users.create_login_url("/"))
            return
        self.redirect("/")

class ApiHandler(BaseHandler):
    def get(self):
        args = dict(
            (name, self.request.get(name).lower().strip()) \
                for name in self.request.arguments() if name != 'q')        
        keywords = [x.lower() for x in self.request.get('q', '').split(',') if x]        
        results = RecordIndex.search(args=args, keywords=keywords)
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(
            simplejson.dumps([simplejson.loads(x.record) for x in results]))        

class LoadTestData(BaseHandler):
    """Loads some test data from a local CSV file."""

    def post(self):
        self.get()
        
    def get(self):
        pkey = Publisher.create('Museum of Vertebrate Zoology').put()
        ckey = Collection.create('Birds', pkey).put()

        start = int(self.request.get('start'))
        size = int(self.request.get('size'))
        logging.info('start=%s, size=%s' % (start, size))
        count = -1
        created = 0
        path = os.path.join(os.path.dirname(__file__), 'data.csv')
        reader = csv.DictReader(open(path, 'r'), skipinitialspace=True)
        dc = []
        dci = []

        while start >= 0:
            reader.next()
            start -= 1

        for rec in reader:
            if created == size:
                model.put_multi(dc)
                model.put_multi(dci)
            rec = dict((k.lower(), v) for k,v in rec.iteritems()) # lowercase all keys
            dc.append(Record.create(rec, ckey))
            dci.append(RecordIndex.create(rec, ckey))
            created += 1
        self.response.out.write('Done. Created %s records' % created)

class BulkloadHandler(BaseHandler):
    @login_required
    def post(self):
        batch = simplejson.loads(self.request.get('batch', None))
        publisher = Publisher.get_by_urlname(urlname(batch.publisher_name))
        collection = Collection.get_by_urlname(
            urlname(batch.collection_name, publisher.key))
        entities = []
        indexes = []
        for rec in batch.recs:
            rec = dict((k.lower(), v) for k,v in rec.iteritems()) # lowercase all keys. do this locally?
            entities.append(Record.create(rec, collection.key))
            indexes.append(RecordIndex.create(rec, collection.key))
        model.put_multi(entities)
        model.put_multi(indexes)
        

class PublisherHandler(BaseHandler):
    def get(self):        
        response = [simplejson.loads(x.json) for x in Publisher.query().fetch()]
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(simplejson.dumps(response))

class PublisherFeedHandler(BaseHandler):
    def get(self, publisher_name):
        publisher = Publisher.get_by_urlname(publisher_name)
        collections = Collection.all_by_publisher(publisher.key)
        response = dict(
            publisher=simplejson.loads(publisher.json),
            collections=[simplejson.loads(x.json) for x in collections])
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(simplejson.dumps(response))

class CollectionHandler(BaseHandler):
    def get(self, publisher_name, collection_name):
        publisher = Publisher.get_by_urlname(publisher_name)
        collection = Collection.get_by_urlname(collection_name, publisher.key)
        response = dict(
            publisher=simplejson.loads(publisher.json),
            collection=simplejson.loads(collection.json))
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(simplejson.dumps(response))

class CollectionFeedHandler(BaseHandler):
    def get(self, publisher_name, collection_name):
        publisher = Publisher.get_by_urlname(publisher_name)
        collection = Collection.get_by_urlname(collection_name, publisher.key)
        logging.info(str(collection))
        records = Record.all_by_collection(collection.key)
        response = dict(
            publisher=simplejson.loads(publisher.json),
            collection=simplejson.loads(collection.json),
            records=[simplejson.loads(x.record) for x in records])
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(simplejson.dumps(response))

class RecordFeedHandler(BaseHandler):
    def get(self, publisher_name, collection_name, occurrence_id):
        self.response.out.write(
            'Publisher=%s, Collection=%s, Record=%s' % \
                (publisher_name, collection_name, occurrence_id))

class BulkloadHandler(BaseHandler):
    def post(self):
        user = users.get_current_user()
        if not user:
            self.error(401)
            return
        logging.info(self.request.body)
        self.response.out.write('hi')

application = webapp.WSGIApplication(
         [('/admin/load', LoadTestData),
          ('/upload', FileUploadHandler),
          ('/upload-form', UploadForm),
          ('/api/search', ApiHandler),
          ('/api/bulkload', BulkloadHandler),
          ('/publishers/?', PublisherHandler),
          ('/publishers/([\w-]+)/?', PublisherFeedHandler),
          ('/publishers/([\w-]+)/([\w-]+)/?', CollectionHandler),
          ('/publishers/([\w-]+)/([\w-]+)/all', CollectionFeedHandler),
          ('/publishers/([\w-]+)/([\w-]+)/(.*)', RecordFeedHandler),
          ],
         debug=True)
         
def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
