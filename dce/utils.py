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

"""This module provides utilities such as unicode DictWriter and DictReader."""

# Standard Python modules
from abc import ABCMeta, abstractmethod, abstractproperty
import codecs
import cStringIO
import csv
import getpass
import logging

# Google App Engine modules
from google.appengine.tools.appengine_rpc import HttpRpcServer

class UTF8Recoder:
    """Iterator that reads an encoded stream and reencodes the input to UTF-8."""
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeDictReader:
    """A CSV reader which will iterate over lines in the CSV file "f", which is 
    encoded in the given encoding.
    """
    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.fieldnames = self.reader.next()

    def next(self):
        row = self.reader.next()
        if len(row) == 0:
            raise StopIteration
        logging.info('row=%s' % row)
        vals = [unicode(s, "utf-8") for s in row]
        return dict((self.fieldnames[x], vals[x]) for x in range(len(self.fieldnames)))

    def __iter__(self):
        return self

class UnicodeDictWriter:
    """A CSV writer which will write rows to CSV file "f", which is encoded in 
    the given encoding.
    """
    def __init__(self, f, fieldnames, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.fieldnames = [x.encode("utf-8") for x in fieldnames]
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()
        
    def writeheader(self):
        self.writer.writerow(self.fieldnames)

    def writerow(self, row):
        self.writer.writerow([row[x].encode("utf-8") for x in self.fieldnames])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class AppEngine(object):
    """Proxy to an App Engine HttpRpcServer."""
    
    class RPC(object):
        """Abstract class for remote procedure calls."""

        __metaclass__ = ABCMeta

        @abstractmethod
        def request_path(self):
            """The path to send the request to, eg /api/appversion/create."""
            pass

        @abstractmethod
        def payload(self):
            """The body of the request, or None to send an empty request"""
            pass

        @abstractmethod
        def content_type(self):
            """The Content-Type header to use."""
            pass

        @abstractmethod
        def timeout(self):
            """Timeout in seconds; default None i.e. no timeout. Note: for large
            requests on OS X, the timeout doesn't work right."""
            pass

        @abstractmethod
        def kwargs(self):
            """Any keyword arguments."""
            pass

    def send(self, rpc):
        return self.server.Send(
            rpc.request_path(),
            rpc.payload(),
            rpc.content_type(),
            rpc.timeout(),
            **rpc.kwargs())

    def __init__(self, host, email, passwd):
        """Initializes the server with user credentials and app details."""
        logging.info('Host %s' % host)
        self.server = HttpRpcServer(
            host,
            lambda:(email, passwd),
            None,
            'vertnet',
            debug_data=True,
            secure=False)

def CredentialsPrompt(host, email=None, passin=False, 
                      raw_input_fn=raw_input, 
                      password_input_fn=getpass.getpass):                                  
    """Prompts the user and returns a username and password."""
    if not email:
        print 'Please enter login credentials for %s' % host
        email = raw_input_fn('Email: ')

    if email:
        password_prompt = 'Password for %s: ' % email
        if passin:
            password = raw_input_fn(password_prompt)
        else:
            password = password_input_fn(password_prompt)
    else:
        password = None

    return email, password

