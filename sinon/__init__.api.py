

from api import *


import os
import threading
import subprocess as sp

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

    sid = None


def initialize (session_id=None) :
    """
    Initialize this instance of the Sinon framework with a session ID.  

    If a session ID is given, then it will be used for all subsequent calls.  
    If none is given, Sinon will attempt to pick it up from the process
    environment (`SINON_SESSION_ID`).  If that also fails, a new (random and
    unique) session ID will be automatically formed and used.  Any Sinon call
    will first make sure that a valid session ID exists.
    """

    with _sinon_rlock :

        global sid
        if  sid :
            sid = session_id

        elif 'SINON_SESSION_ID' in os.environ :
            sid = os.environ['SINON_SESSION_ID']

        else :
            sid = sui.generate_session_id ()


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

