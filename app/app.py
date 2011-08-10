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
from ndb import query

from models import Publisher, Collection, Record, RecordIndex
try:
    appid = os.environ['APPLICATION_ID']
    appver = os.environ['CURRENT_VERSION_ID'].split('.')[0]
except:
    pass


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
        others = ['offset', 'limit', 'q']
        args = dict(
            (name, self.request.get(name).lower().strip()) \
                for name in self.request.arguments() if name not in others)        
        keywords = [x.lower() for x in self.request.get('q', '').split(',') if x]
        limit = self.request.get_range('limit', min_value=1, max_value=100, default=10)
        offset = self.request.get_range('offset', min_value=0, default=0)
        results = RecordIndex.search(limit, offset, args=args, keywords=keywords)
        logging.info(str(results))
        self.response.headers["Content-Type"] = "application/json"
        self.response.out.write(
            simplejson.dumps([simplejson.loads(x.json) for x in results]))        

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
        #logging.info(self.request.body)
        self.response.out.write(self.request.body)

application = webapp.WSGIApplication(
    [
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
