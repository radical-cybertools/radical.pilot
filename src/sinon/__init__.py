
VERSION = 'v1'

from v1 import *

import os
import datetime
import threading
import subprocess

import radical.utils as ru
import saga.advert   as sa




# ------------------------------------------------------------------------------
#
_rlock = threading.RLock ()


# ------------------------------------------------------------------------------
#
with _rlock :

    version = 'latest'

    try:
        cwd = os.path.dirname(os.path.abspath(__file__))
        fn = os.path.join(cwd, 'VERSION')
        version = open(fn).read().strip()
    except IOError:
        from subprocess import Popen, PIPE, STDOUT
        import re

        VERSION_MATCH = re.compile(r'\d+\.\d+\.\d+(\w|-)*')

        try:
            p = Popen(['git', 'describe', '--tags', '--always'],
                      stdout=PIPE, stderr=STDOUT)
            out = p.communicate()[0]

            if (not p.returncode) and out:
                v = VERSION_MATCH.search(out)
                if v:
                    version = v.group()
        except OSError:
            pass


# ------------------------------------------------------------------------------
#
with _rlock :

    sid          = None
    root_dir     = None
    _initialized = False


def initialize (session_id=None, root_url=None) :
    """
    Initialize this instance of the Sinon framework with a session ID, and
    a root coordination URL. 

    If a session ID is given, then it will be used for all subsequent calls.  
    If none is given, Sinon will attempt to pick it up from the process
    environment (`SINON_SESSION_ID`).  If that also fails, a new (random and
    unique) session ID will be automatically formed and used.  Any Sinon call
    will first make sure that a valid session ID exists.

    The root coordination url will be formed according to the Sinon data model
    (see v1/README.md)::

      redis://gw68.quarry.iu.teragrid.org/sinon/v1/users/<username>/<sid>/

    where `<username>` is the local system user id, and `<sid>` is the session
    ID from above.  

    """

    with _rlock :

        global sid
        global root_dir
        global _initialized


        # initialize only once
        if  _initialized :
            return (sid, root_dir)

       
        # create (or pick-up) unique session ID
        if  session_id                        : sid = session_id
        elif 'SINON_SESSION_ID' in os.environ : sid = os.environ['SINON_SESSION_ID']
        else                                  : sid = ru.generate_id ('s.')

        print "Sinon session ID : %s" % sid

        if   'SINON_USER_ID' in os.environ : user_id    = os.environ['SINON_USER_ID']
        elif 'USER'          in os.environ : user_id    = os.environ['USER']
        elif 'USERNAME'      in os.environ : user_id    = os.environ['USERNAME']
        else                               : user_id    = os.getuid ()

        print "Sinon user    ID : %s" % user_id

        if 'REDIS_URL' in os.environ       : redis_url  = os.environ['REDIS_URL']
        else                               : redis_url  = 'redis://gw68.quarry.iu.teragrid.org'
        if  not redis_url.endswith ('/')   : redis_url += '/'

        # create (or pick-up) root url
        if   root_url                       : root_url = Url (root_url)
        elif 'SINON_ROOT_URL' in os.environ : root_url = Url (os.environ['SINON_ROOT_URL'])
        else                                : root_url = Url ("%ssinon/%s/users/%s/%s/" \
                                                       % (redis_url, VERSION, user_id, sid))

        print "Sinon session URL: %s" % str(root_url)

        # make sure the sesison URL is valid
        root_dir = sa.Directory (str(root_url), sa.CREATE_PARENTS)
      # root_dir.set_attribute  ('created', str(datetime.datetime.utcnow ()))

        # we will not need to initialize ever again
        _initialized = True

        
        return (sid, root_dir)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

