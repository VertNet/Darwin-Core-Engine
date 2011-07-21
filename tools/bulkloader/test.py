import appcfg
appcfg.fix_sys_path()

import sys
sys.path = ['../../app'] + sys.path

from ndb import model
from ndb import query

from app import Record

if __name__ == '__main__':
    key = model.Key('Foo', 'bar')
    print str(key)
    urlsafe = key.urlsafe()
    print urlsafe
    print model.Key(urlsafe=urlsafe)
