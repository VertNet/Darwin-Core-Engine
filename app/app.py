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

# Standard Python imports
import csv
import logging
import os
import simplejson
import urllib

# Google App Engine imports
from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import deferred
from google.appengine.ext import webapp
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.api import users
from google.appengine.api import taskqueue
from google.appengine.api import urlfetch
from google.appengine.ext.webapp.util import login_required
from google.appengine.datastore import datastore_rpc
from google.appengine.datastore import entity_pb
from google.appengine.api import memcache

# DCE imports
from dce import concepts

# Datastore Plus imports
from ndb import query, model, context, tasklets

from models import Publisher, Collection, Record, RecordIndex

# Set current appid and version
try:
    appid = os.environ['APPLICATION_ID']
    appver = os.environ['CURRENT_VERSION_ID'].split('.')[0]
except:
    pass

# Set wether we are in production or development
if 'SERVER_SOFTWARE' in os.environ:
    PROD = not os.environ['SERVER_SOFTWARE'].startswith('Development')
else:
    PROD = True

# Set couchdb instance
if PROD:
    COUCHDB = 'vertnet-prod'
else:
    COUCHDB = 'vertnet-dev'

# ------------------------------------------------------------------------------#
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

class CouchDb(object):
    @classmethod
    def query_bb(cls, bb): # TODO add paging support or count constraints
        url='http://eighty.iriscouch.com/%s/_design/main/_spatial/points?bbox=%s' % (COUCHDB, bb)
        logging.info(url)
        response = urlfetch.fetch(
            url=url,
            method=urlfetch.GET)
        if response.status_code != 200:
            logging.info('NO RESULTS')
            return []
        keys = [model.Key(urlsafe=row['id']) for row in simplejson.loads(response.content).get('rows')]
        return model.get_multi(keys)

class ApiHandler(BaseHandler):
    
    class DarwinCoreRequest(object):
        """Class for handling a Darwin Core request."""
        
        @classmethod
        def handle(cls, handler):
            params = cls.validate_request(handler.request)
            if not params:
                cls.error(404)            
            m_key = str(params)
            logging.info('key=%s' %  m_key)
            response = memcache.get(m_key)
            if response:
                handler.response.headers["Content-Type"] = "application/json"
                handler.response.out.write(response)
                return                
            results, cursor, more = RecordIndex.search(params)
            records = '[%s]' % ','.join([x.json for x in results])
            response = '{"records":%s' % records
            if cursor and more:
                offset = cursor.to_websafe_string()
                response = '%s, "next_offset":"%s"}' % (response, offset)
            else:
                response = '%s, "next_offset":null}' % response
            memcache.add(m_key, response)
            handler.response.headers["Content-Type"] = "application/json"
            handler.response.out.write(response)

        @classmethod
        def validate_request(cls, request):
            args = cls.get_dwc_args(request)
            if len(args) == 0 and not request.get('q', None):
                return False
            keywords = [x.lower() for x in request.get('q', '').split(',') if x]
            limit = request.get_range('limit', min_value=1, max_value=100, default=10)
            offset = request.get('offset', None)
            cursor = None
            if offset:
                cursor = Cursor.from_websafe_string(offset)
            return dict(
                args=args, 
                keywords=keywords, 
                limit=limit, 
                offset=offset, 
                cursor=cursor)

        @classmethod
        def get_dwc_args(cls, request):
            """Return dictionary of Darwin Core short names to values."""
            args = dict()
            for arg in request.arguments():
                short_name = None
                if concepts.is_short_name(arg):
                    short_name = arg
                elif concepts.is_name(arg):
                    short_name = concepts.get_short_name(arg)
                if short_name:
                    args[short_name] = request.get(arg).strip().lower()
            return args

        @classmethod
        def error(cls, error_code, handler):  
            logging.info('Bad request')
            handler.error(error_code)            
            reason = 'Invalid parameters'
            handler.render_template(
                '404.html', 
                dict(request_path=handler.request.query_string, reason=reason))

    def get(self):
        # Handle bbox request and return
        bb = self.request.get('bb', None)
        if bb:
            results = CouchDb.query_bb(bb)
            self.response.headers["Content-Type"] = "application/json"
            self.response.out.write(
                simplejson.dumps([simplejson.loads(x.json) for x in results]))        
            return
        
        ApiHandler.DarwinCoreRequest.handle(self)


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
    def get(self):
        pkey = model.Key('Publisher', 'MVZ')
        Publisher(id='MVZ', name='MVZ', json='{}').put()
        ckey = model.Key('Collection', 'Birds', parent=pkey)            
        Collection(id='Birds', name='Birds', json='{}').put()

application = webapp.WSGIApplication(
    [   
        ('/pop', BulkloadHandler),
        ('/upload', FileUploadHandler),
        ('/upload-form', UploadForm),
        ('/api/search', ApiHandler),
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
