# Copyright 2011 Aaron Steele
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

__author__ = "Aaron Steele"

"""This module handles listserve type emails for VertNet.

The VertNet project maintains a public Google Fusion Table of publishers,
collections, and contacts:

http://www.google.com/fusiontables/DataSource?dsrcid=766366

This module supports sending aggregate emails to VertNet contact groups that 
are stored in the Fusion Table. 

Currently supported email addresses:
    everyone@vert-net.appspotmail.com - All contacts.
    admins@vert-net.appspotmail.com - Contacts with the 'Administrator' role.
    techs@vert-net.appspotmail.com - Contacts with the 'Technical' role.
"""

import email
import logging
import simplejson
import urllib

from google.appengine.api import mail
from google.appengine.api import urlfetch
from google.appengine.ext import webapp 
from google.appengine.ext.webapp.mail_handlers import InboundMailHandler 
from google.appengine.ext.webapp.util import run_wsgi_app

FUSION_TABLE_ID = '766366'
COLUMNS = "PersonName, PersonEmail, PersonRole, Birds, Fish, Herps, Mammals"
SQL = 'SELECT %s FROM %s' % (COLUMNS, FUSION_TABLE_ID)
PARAMS = urllib.urlencode({'sql': SQL, 'jsonCallback': 'foo'})
URL = 'http://www.google.com/fusiontables/api/query?%s' % PARAMS
AUTHORIZED_SENDERS = ['eightysteele@gmail.com', 'gtuco.btuco@gmail.com', 'noreply@googlegroups.com']

def getaddrs(data):
    """Returns a list of email address names (stuff before the '@').
    
    Args:
        data - list of email addresses.
    """
    if not data:
        return []
    return [x.split('@')[0].split()[-1].replace('<','')
            for x in data.split(',')]

def getftdata():
    """Returns the Fusion Table as a JSON object."""
    result = urlfetch.fetch(URL)
    if result.status_code == 200:
        json = result.content.replace('foo(', '').replace(')', '')
        return simplejson.loads(json)
    
def getuniques(vals):
    """Returns a list of unique values.
    
    Args:
        vals - A list of strings.
    """
    d = {}
    for x in vals:
        d[x] = None
    return d.keys()

class EmailHandler(InboundMailHandler):
            
    def receive(self, msg):
        logging.info('Received mail from %s' % msg.sender)
        if msg.sender not in AUTHORIZED_SENDERS:
            return
        
        # Forwards messages from google groups to Aaron:
        if msg.sender == 'noreply@googlegroups.com':
            logging.info('Forwarding message to eighty')
            mail.send_mail(sender='noreply@vert-net.appspotmail.com',
                           to='eightysteele@gmail.com',
                           subject=msg.subject,
                           body=msg.body)
            return  

        data = getftdata()
        addrs = getuniques(getaddrs(msg.to))
        try:
            addrs += getuniques(getaddrs(msg.cc))
        except:
            pass
        
        to = []

        # Grabs all the email addresses based on addr name (everyone, techs, admins, etc):
        for addr in addrs:
            if addr == 'everyone':
                to += ['%s <%s>' % (x[0], x[1]) for x in data['table']['rows']]
            elif addr == 'admins':
                to += ['%s <%s>' % (x[0], x[1]) for x in data['table']['rows']
                       if x[2] == 'Administrative']
            elif addr == 'techs':
                to += ['%s <%s>' % (x[0], x[1]) for x in data['table']['rows']
                       if x[2] == 'Technical']
                
        # Handles invalid to address by bouncing back to sender:
        if len(to) == 0:
            mail.send_mail(sender='admin@vert-net.appspotmail.com',
                           to=msg.sender,
                           subject='Oops! Unable to send VertNet email',
                           body='The following email addresses are invalid: %s' % msg.to)
            return
        
        # Gets uniques and sends the email:
        to = getuniques(to)
        mail.send_mail(sender=msg.sender,
                       to=reduce(lambda x,y: '%s,%s' % (x, y), to),
                       subject=msg.subject,
                       body=msg.body)

application = webapp.WSGIApplication([EmailHandler.mapping()], debug=True)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
