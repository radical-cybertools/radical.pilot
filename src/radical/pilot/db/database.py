
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import sys
import copy
import time
import pymongo

import radical.utils  as ru
import saga           as rs

from .. import states as rps
from .. import utils  as rpu


# ------------------------------------------------------------------------------
#
class DB(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg, connect=True):
        '''
        Creates a new database connection 

        A session is a MongoDB collection which contains documents of
        different types:

        session : document describing this rp.Session (singleton)
        pmgr    : document describing a rp.PilotManager 
        pilots  : document describing a rp.Pilot
        umgr    : document describing a rp.UnitManager
        units   : document describing a rp.Unit
        '''

        self._session    = session
        self._sid        = self._session.uid
        self._log        = self._session.get_logger(name=self._sid)

        self._cfg        = cfg
        self._created    = time.time()
        self._connected  = None
        self._closed     = None
        self._c          = None

        if not connect:
            return

        py_version_detail = sys.version.replace("\n", " ")
        version_info = {'radical_stack' : {'rp': rpu.version_detail,
                                           'rs':  rs.version_detail,
                                           'ru':  ru.version_detail,
                                           'py':  py_version_detail}}
        self._connected = time.time()


# ------------------------------------------------------------------------------

