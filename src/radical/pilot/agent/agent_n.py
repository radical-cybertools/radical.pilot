
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import pprint

import radical.utils      as ru

from ..  import db
from ..  import utils     as rpu
from ..  import constants as rpc
from ..  import Session   as rp_Session


# ==============================================================================
#
class Agent_n(rpu.Worker):

    # This is a sub-agent.  It does not do much apart from starting
    # agent components and watching them  If any of the components die, 
    # it will shut down the other components and itself.  
    #
    # This class inherits the rpu.Worker, so that it can use the communication
    # bridges and callback mechanisms.  It will own a session (which knows said
    # communication bridges (or at least some of them); and a controller, which
    # will control the components.

    # --------------------------------------------------------------------------
    #
    def __init__(self, agent_name):

        assert(agent_name != 'agent_0'), 'expect subagent, not agent_0'
        print "startup agent %s" % agent_name

        # load config, create session and controller, init rpu.Worker
        agent_cfg = "%s/%s.cfg" % (os.getcwd(), agent_name)
        self._cfg = ru.read_json_str(agent_cfg)
        self._uid = agent_name
        self._pid = self._cfg['pilot_id']
        self._sid = self._cfg['session_id']

        self._final_cause = None

        # Create a session.  
        #
        # This session will not connect to MongoDB, but will create any
        # communication channels and components/workers specified in the 
        # config -- we merge that information into our own config.
        # We don't want the session to start components though, so remove them
        # from the config copy.
        session_cfg = copy.deepcopy(cfg)
        session_cfg['components'] = dict()
        session = rp_Session(uid=self._sid, _cfg=session_cfg)

        # connect to the DB, start bridges and components
        self._cfg['uid']   = self._uid
        self._cfg['owner'] = self._session.uid
        self._cfg['dburl'] = self._session._cfg['dburl']

        self._db   = db.DB(self._session, cfg=self._cfg)
        self._cmgr = rpu.ComponentManager(self._session, self._cfg, self._uid)


        # at this point the session is up and workin, and the session
        # controller should have brought up all communication bridges and the
        # agent components.  We are ready to roll!
        rpu.Worker.__init__(self, self._cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # once is done, we signal success to the parent agent
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'alive',
                                          'arg' : {'sender' : self._uid, 
                                                   'owner'  : self._owner, 
                                                   'src'    : 'agent'}})


    # --------------------------------------------------------------------------
    #
    def wait_final(self):

        while self._final_cause is None:
          # self._log.info('no final cause -> alive')
            time.sleep(0.1)

        self._log.debug('final: %s', self._final_cause)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # tear things down in reverse order
        if self._session:
            self._log.debug('close  session %s', self._session.uid)
            self._session.close()
            self._log.debug('closed session %s', self._session.uid)

        self._final_cause = 'finalized'


# ------------------------------------------------------------------------------

