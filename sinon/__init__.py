
VERSION = 'v1'

from   bj import *

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

    version = "unknown"
    
    try :
        cwd     = os.path.dirname (os.path.abspath (__file__))
        fn      = os.path.join    (cwd, 'VERSION')
        version = open (fn).read ().strip ()
    
        p   = subprocess.Popen (['git', 'describe', '--tags', '--always'],
                        stdout=subprocess.PIPE)
        out = p.communicate()[0]
    
        # ignore pylint error on p.returncode -- false positive
        if  out and not p.returncode :
            version += '-' + out.strip()
    
    except Exception :
        pass
    

# ------------------------------------------------------------------------------
#
with _rlock :

    sid          = None
    _initialized = False


def initialize (session_id=None) :
    """
    Initialize this instance of the Sinon framework with a session ID.

    If a session ID is given, then it will be used for all subsequent calls.  
    If none is given, Sinon will attempt to pick it up from the process
    environment (`SINON_SESSION_ID`).  If that also fails, a new (random and
    unique) session ID will be automatically formed and used.  Any Sinon call
    will first make sure that a valid session ID exists.
    """


    with _rlock :

        global sid
        global _initialized


        # initialize only once
        if  _initialized :
            return sid

       
        if   'SINON_USER_ID' in os.environ : user_id    = os.environ['SINON_USER_ID']
        elif 'USER'          in os.environ : user_id    = os.environ['USER']
        elif 'USERNAME'      in os.environ : user_id    = os.environ['USERNAME']
        else                               : user_id    = os.getuid ()

        print "Sinon user    ID : %s" % user_id


        if  session_id                        : sid = session_id
        elif 'SINON_SESSION_ID' in os.environ : sid = os.environ['SINON_SESSION_ID']
        else                                  : sid = ru.generate_id ('s.%s.' % user_id)

        print "Sinon session ID : %s" % sid

        # we will not need to initialize ever again
        _initialized = True
        
        return sid



# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

