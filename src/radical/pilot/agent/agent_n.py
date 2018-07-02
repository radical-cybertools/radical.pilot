
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import copy
import stat
import time
import pprint

import radical.utils      as ru

from ..  import utils     as rpu
from ..  import states    as rps
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
        agent_cfg  = "%s/%s.cfg" % (os.getcwd(), agent_name)
        cfg        = ru.read_json_str(agent_cfg)

        self._uid         = agent_name
        self._pid         = cfg['pilot_id']
        self._sid         = cfg['session_id']
        self._final_cause = None

        # Create a session.  
        #
        # This session will not connect to MongoDB, but will create any
        # communication channels and components/workers specified in the 
        # config -- we merge that information into our own config.
        # We don't want the session to start components though, so remove them
        # from the config copy.
        session_cfg = copy.deepcopy(cfg)
        session_cfg['owner']      = self._uid
        session_cfg['components'] = dict()
        session = rp_Session(cfg=session_cfg, _connect=False, uid=self._sid)

        # we still want the bridge addresses known though, so make sure they are
        # merged into our own copy, along with any other additions done by the
        # session.
        ru.dict_merge(cfg, session._cfg, ru.PRESERVE)
        pprint.pprint(cfg)

        if session.is_connected:
            raise RuntimeError('agent_n should not connect to mongodb')

        # at this point the session is up and workin, and the session
        # controller should have brought up all communication bridges and the
        # agent components.  We are ready to roll!
        rpu.Worker.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):

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
            time.sleep(1)

        self._log.debug('final: %s', self._final_cause)


    # --------------------------------------------------------------------------
    #
    def finalize_parent(self):

        # tear things down in reverse order
        if self._session:
            self._log.debug('close  session %s', self._session.uid)
            self._session.close()
            self._log.debug('closed session %s', self._session.uid)

        self._final_cause = 'finalized'


# ------------------------------------------------------------------------------

