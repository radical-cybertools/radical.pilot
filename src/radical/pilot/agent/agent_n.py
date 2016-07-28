
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import copy
import stat
import time
import pprint
import subprocess

import radical.utils as ru

from ..  import utils     as rpu
from ..  import states    as rps
from ..  import constants as rpc

from ..session  import Session as rp_Session

# ==============================================================================
#
class Agent_n(rpu.Worker):
    """
    This is an sub-agent to Agent_0
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        self.final_cause  = None
        self._lrms        = None

        self._uid         = cfg['agent_name']
        self._agent_name  = cfg['agent_name']
        self._session_id  = cfg['session_id']
        self._pilot_id    = cfg['pilot_id']

        # we don't want the session controller to start our components just yet
        session_cfg = copy.deepcopy(cfg)
        session_cfg['components'] = dict()
        session = rp_Session(cfg=session_cfg, uid=self._session_id, _connect=False)
        ru.dict_merge(cfg, session.ctrl_cfg, ru.PRESERVE)

        rpu.Worker.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._log.debug('starting AgentWorker for %s' % self.uid)


    # --------------------------------------------------------------------------
    #
    @property
    def agent_name(self):
        return self._agent_name


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        Read the configuration file, setup logging and mongodb connection.
        This prepares the stage for the component setup (self._setup()).
        """

        self._runtime    = self._cfg['runtime']
        self._starttime  = time.time()

        self._controller = rpu.Controller(cfg=self._cfg, session=self._session)

        self._prof.prof('Agent setup done', logger=self._log.debug, uid=self._pilot_id)

        # once bootstrap_5 is done, we signal success to the parent agent
        # -- if we have any parent...
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'alive',
                                          'arg' : {'sender' : self.agent_name, 
                                                   'owner'  : self._cfg['owner']}})


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        self._log.info("Agent finalizes")
        self._prof.prof('stop', uid=self._pilot_id)

        if self._lrms:
            self._lrms.stop()

        self._log.info("Agent finalized")


# ------------------------------------------------------------------------------

