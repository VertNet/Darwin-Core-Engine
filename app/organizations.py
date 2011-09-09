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

import logging
import os
import simplejson

from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app, login_required

from ndb.model import Model, StructuredProperty, UserProperty, StringProperty, Key, ComputedProperty
from ndb import query, model, context, tasklets

try:
    appid = os.environ['APPLICATION_ID']
    appver = os.environ['CURRENT_VERSION_ID'].split('.')[0]
except:
    pass

if 'SERVER_SOFTWARE' in os.environ:
    PROD = not os.environ['SERVER_SOFTWARE'].startswith('Development')
    HOST = 'localhost:8080'
else:
    PROD = True
    HOST = '%s.vert-net.appspot.com' % appid


def urlname(name):
    """Returns a URL friendly name by replacing space with '-' char."""
    return '-'.join(''.join(ch.lower() for ch in word if ch.isalnum()) \
                        for word in name.split())

# ------------------------------------------------------------------------------
# Models

class Member(Model):
    user = UserProperty('u', required=True)

    # JSON computed property
    #json = ComputedProperty(lambda self: self.user.email())
    
    @classmethod
    def build(cls, email):
        return Member(user=users.User(email=email))

class Collection(Model): 
    name = StringProperty('n', required=True)
    team = StringProperty('t', required=True)
    permission = StringProperty('p', required=True)
    member = StructuredProperty(Member, 'm', required=True)
    
    # JSON computed property
    def to_json(self):
        return dict(
            name=self.name, 
            team=self.team,
            permission=self.permission,
            member=self.member.user.email())

    @classmethod
    def build(cls, name, team, permission, member):
        return Collection(
            name=name, 
            team=team, 
            permission=permission, 
            member=Member.build(member))

    @classmethod
    def build_from_json(cls, json):
        return cls.build(
            json['name'], 
            json['team'], 
            json['permission'], 
            json['member'])
            
class Organization(Model):
    name = StringProperty('n', required=True)
    urlname = model.ComputedProperty(lambda self: urlname(self.name))
    owners = StructuredProperty(Member, 'o', repeated=True)
    members = StructuredProperty(Member, 'm', repeated=True)
    collections = StructuredProperty(Collection, 'c', repeated=True)

    def to_json(self):
        return dict(
            name=self.name,
            owners=[owner.user.email() for owner in self.owners],
            members=[member.user.email() for member in self.members],
            collections=[collection.to_json() for collection in self.collections])

    @classmethod
    def create_or_update(cls, name, owners, members=[], collections=[]):
        return Organization(
            id=urlname(name), 
            name=name, 
            owners=[Member.build(email) for email in owners], 
            members=[Member.build(email) for email in members], 
            collections=[Collection.build_from_json(c) for c in collections]).put() 

    @classmethod
    def create_from_json(cls, json):        
        """Creates an Organiation from a JSON object and returns the key."""
        return cls.create(
            json['name'], 
            json['owners'],
            members=json.get('members', []), 
            collections=json.get('collections', []))
        
    @classmethod
    def get_by_name(cls, name):
        return cls.get_by_urlname(urlname(name))

    @classmethod
    def key_by_name(cls, name):
        return Key(cls.__name__, urlname(name))

    @classmethod
    def get_by_urlname(cls, urlname):
        return Key(cls.__name__, urlname).get()

    def update_owners(self, add=[], remove=[]):
        self.owners.extend([Member.build(email) for email in add])
        for owner in [Member.build(email) for email in remove]:
            if owner in self.owners:
                self.owners.remove(owner)
        return self

    def update_members(self, add=[], remove=[]):
        self.members.extend([Member.build(email) for email in add])
        for member in [Member.build(email) for email in remove]:
            if member in self.members:
                self.members.remove(member)
        return self

    def update_collections(self, add=[], remove=[]):
        self.collections.extend([Collection.build_from_json(c) for c in add])
        for collection in [Collection.build_from_json(c) for c in remove]:
            if collection in self.collections:
                self.collections.remove(collection)
        return self


# ------------------------------------------------------------------------------
# Organizations

class GetOrganization(webapp.RequestHandler):
    def get(self, urlname):
        o = Organization.get_by_urlname(urlname)
        if not o:
            self.error(404)
            return
        self.response.set_status(200)
        self.response.headers['Content-Type'] = "application/json"
        self.response.out.write(simplejson.dumps(o.to_json()))

class CreateOrganization(webapp.RequestHandler):
    def get(self):
        self.error(405)
        self.response.headers['Allow'] = 'POST'
        return
    
    def post(self):
        # Validate name
        name = self.request.get('name', None)        
        if not name:
            self.error(400)
            return

        # Check user authentication
        user = users.get_current_user()
        if not user:
            self.error(401)
            return

        # Check if organization exists
        organization = Organization.get_by_name(name)
        if organization:
            self.error(403)
            self.response.headers['Content-Type'] = "application/json"
            self.response.out.write('{"error": "Organization %s exists"}\n' % name)
            return            
        
        # Create organization
        Organization.create_or_update(name, [user.email()], members=[user.email()])

        # Return response
        uri = 'http://%s/organizations/%s\n' % (HOST, urlname(name))
        self.response.set_status(201)
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.headers['Content-Location'] = uri
        self.response.out.write(uri)

class UpdateOrganization(webapp.RequestHandler):
    def get(self, organization):
        logging.error('Update organization via GET')
        self.error(405)
        self.response.headers['Allow'] = 'POST'
        return
        
    def post(self, urlname):
        # Check user authentication
        user = users.get_current_user()
        if not user:
            self.error(401)
            return

        # Check if organization exists
        organization = Organization.get_by_urlname(urlname)
        if not organization:
            self.error(403)
            self.response.headers['Content-Type'] = "application/json"
            self.response.out.write('{"error": "Organization %s does not exists"}\n' % urlname)
            return            
        
        # Check if user is owner
        owner = Member(user=user)
        if owner not in organization.owners:
            self.error(401)
            return

        # Get owners from request
        owners = []
        o = self.request.get('owners', None)
        if o:
            owners.extend(o.split(','))
            
        # Get members from request
        members = []
        m = self.request.get('members', None)
        if m:
            members.extend(m.split(','))
        
        # Get collections from request
        collections = []
        c = self.request.get_all('collection')
        for x in c:
            collections.append(
                dict((key,value) for key,value in \
                         zip(['name', 'team', 'permission', 'member'], x.split(','))))
        
        # Update organization
        organization = Organization.create_or_update(
            organization.name, owners, members=members, collections=collections).get()

        # Send response
        self.response.set_status(200)
        self.response.headers['Content-Type'] = "application/json"
        self.response.out.write(simplejson.dumps(organization.to_json()))
        
class DeleteOrganization(webapp.RequestHandler):
    def get(self, organization):
        self.error(405)
        self.response.headers['Allow'] = 'POST'
        return

    def post(self, organization):
        # Validate name
        name = self.request.get('name', None)        
        if not name:
            self.error(400)
            return

        # Check user authentication
        user = users.get_current_user()
        if not user:
            self.error(401)
            return

        # Check if organization exists
        organization = Organization.get_by_name(name)
        if not organization:
            self.error(403)
            self.response.headers['Content-Type'] = "application/json"
            self.response.out.write('{"error": "Organization %s does not exist"}\n' % name)
            return            
        
        owner = Member(user=user)
        if owner not in organization.owners:
            self.error(401)
            return
        
        # Delete the organization
        organization.key.delete()
        self.response.set_status(200)

class ListOrganizations(webapp.RequestHandler):
    def get(self):
        #TODO
        self.response.headers['Content-Type'] = "application/json"
        self.response.out.write('[{},{},{}]')
    
# ------------------------------------------------------------------------------
# Collections within an Organization

class GetCollection(webapp.RequestHandler):
    def get(self, organization, collection):
        self.response.headers['Content-Type'] = "application/json"
        self.response.out.write(
            '{"organization": "%s", "collection": "%s"}' % (organization, collection))

class ListCollections(webapp.RequestHandler):
    def get(self, organization):
        self.response.headers['Content-Type'] = "application/json"
        self.response.out.write(
            '{"organization": "%s", "collections": []}' % organization)

class CreateCollection(webapp.RequestHandler):
    def get(self, organization):
        self.error(405)
        self.response.headers['Allow'] = 'POST'
        return
    
    @login_required
    def post(self):
        # TODO
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write(name)

class UpdateCollection(webapp.RequestHandler):
    def get(self, organization, collection):
        logging.error('Update %s/%s via GET' % (organization, collection))
        self.error(405)
        self.response.headers['Allow'] = 'POST'
        return
        
    @login_required
    def post(self, organization, collection):
        # TODO
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write(name)
        
class DeleteCollection(webapp.RequestHandler):
    def get(self, organization, collection):
        self.error(405)
        self.response.headers['Allow'] = 'POST'
        return

    @login_required
    def post(self, organization, collection):
        # TODO
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.out.write(name)
            
        
application = webapp.WSGIApplication(
    [
     
     # Organization Permissions
     ('/organizations/[(.*)]/permissions/{0,1}$', GetOrganization), 
     
     # Organizations
     ('/organizations/create', CreateOrganization),
     ('/organizations/([a-zA-Z\-]+)/?', GetOrganization),
     ('/organizations/?', ListOrganizations),
     ('/organizations/([a-zA-Z\-]*?)/update', UpdateOrganization), 
     ('/organizations/([a-zA-Z\-]*?)/delete', DeleteOrganization), 
     
     # Collections
     ('/organizations/([a-zA-Z\-]*?)/([a-zA-Z\-]*?)/?', GetCollection),
     ('/organizations/([a-zA-Z\-]*?)/collections/?', ListCollections),
     ('/organizations/([a-zA-Z\-]*?)/collections/create', CreateCollection),
     ('/organizations/([a-zA-Z\-]*?)/([a-zA-Z\-]*?)/update', UpdateCollection), 
     ('/organizations/([a-zA-Z\-]*?)/([a-zA-Z\-]*?)/delete', DeleteCollection), 

     ],
     debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
