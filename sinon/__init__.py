
VERSION = 'v1'

from v1 import *

import saga.advert     as sa

import os
import threading
import subprocess      as sp

import sinon.utils.ids as sui


# ------------------------------------------------------------------------------
#
_sinon_rlock = threading.RLock ()


# ------------------------------------------------------------------------------
#
with _sinon_rlock :

    version = "unknown"
    
    try :
        cwd     = os.path.dirname (os.path.abspath (__file__))
        fn      = os.path.join    (cwd, 'VERSION')
        version = open (fn).read ().strip ()
    
        p   = sp.Popen (['git', 'describe', '--tags', '--always'],
                        stdout=sp.PIPE)
        out = p.communicate()[0]
    
        # ignore pylint error on p.returncode -- false positive
        if  out and not p.returncode :
            version += '-' + out.strip()
    
    except Exception :
        pass
    


# ------------------------------------------------------------------------------
#
with _sinon_rlock :

    sid          = None
    base_url     = None
    _initialized = False


def initialize (session_id=None, base_url=None) :
    """
    Initialize this instance of the Sinon framework with a session ID, and
    a base coordination URL. 

    If a session ID is given, then it will be used for all subsequent calls.  
    If none is given, Sinon will attempt to pick it up from the process
    environment (`SINON_SESSION_ID`).  If that also fails, a new (random and
    unique) session ID will be automatically formed and used.  Any Sinon call
    will first make sure that a valid session ID exists.

    The base coordination url will be formed according to the Sinon data model
    (see v1/README.md)::

      redis://gw68.quarry.iu.teragrid.org/sinon/v1/users/<username>/<sid>/

    where `<username>` is the local system user id, and `<sid>` is the session
    ID from above.  

    """

    with _sinon_rlock :

        global sid
        global _initialized


        # initialize only once
        if  _initialized :
            return (sid, base_url)

       
        # create (or pick-up) unique session ID
        if  sid :
            sid = session_id

        elif 'SINON_SESSION_ID' in os.environ :
            sid = os.environ['SINON_SESSION_ID']

        else :
            sid = sui.generate_session_id ()

        print "Sinon session ID : %s" % sid


        # create (or pick-up) base url
        if  base_url :
            base_url = Url (base_url)

        elif 'SINON_BASE_URL' in os.environ :
            base_url = Url (os.environ['SINON_BASE_URL'])

        else :
            redis_url = 'redis://gw68.quarry.iu.teragrid.org'
            user_id   = os.getuid ()

            if 'REDIS_URL' in os.environ : redis_url = os.environ['REDIS_URL']
            if 'USER'      in os.environ : user_id   = os.environ['USER']
            if 'USERNAME'  in os.environ : user_id   = os.environ['USERNAME']

            base_url = Url ("%s/sinon/%s/users/%s/%s/" \
                         % (redis_url, VERSION, user_id, sid))

        print "Sinon session URL: %s" % base_url

        # make sure the sesison URL is valid
        base_dir = sa.Directory (str(base_url), sa.CREATE_PARENTS)
        base_dir.set_attribute  ('key', 'val')


        # we will not need to initialize ever again
        _initialized = True

        return (sid, base_url)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

