
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import radical.utils  as ru

from .. import utils as rpu

from .. import Session


# ------------------------------------------------------------------------------
#
class Agent_n(rpu.AgentComponent):

    # This is a sub-agent.  It does not do much apart from starting
    # agent components and watching them, which is all taken care of in the
    # `AgentComponent` base class.

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, reg_addr, uid):

        reg = ru.zmq.RegistryClient(url=reg_addr)
        cfg = ru.Config(cfg=reg['cfg'])

        # use our own config sans agents/components/bridges as a basis for
        # the sub-agent config.
        a_cfg = cfg['agents'][uid]

        self._uid     = uid
        self._session = Session(uid=sid, cfg=a_cfg,
                                _reg_addr=reg_addr,
                                _role=Session._AGENT_N)

        self._pid     = self._session.cfg.pid
        self._sid     = self._session.cfg.sid
        self._owner   = self._session.cfg.owner
        self._pmgr    = self._session.cfg.pmgr
        self._pwd     = self._session.cfg.pilot_sandbox

        # init the agent component base classes, connects registry
        super().__init__(self._cfg, self._session)



    # --------------------------------------------------------------------------
    def finalize(self):

        self._prof.prof('sub_agent_stop', uid=self._pid, msg=self._uid)


# ------------------------------------------------------------------------------

