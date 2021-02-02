
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import radical.utils  as ru

from ..  import utils as rpu


# ------------------------------------------------------------------------------
#
class Agent_n(rpu.Worker):

    # This is a sub-agent.  It does not do much apart from starting
    # agent components and watching them, which is all taken care of in the
    # `Worker` base class (or rather in the `Component` base class of `Worker`).

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._cfg     = cfg
        self._pid     = cfg.pid
        self._pmgr    = cfg.pmgr
        self._pwd     = cfg.pilot_sandbox

        # log / profile via session until component manager is initialized
        self._session = session
        self._log     = session._log
        self._prof    = session._prof

        self._starttime   = time.time()
        self._final_cause = None

        # this is the earliest point to sync bootstrap and agent profiles
        self._prof.prof('hostname', uid=self._pid, msg=ru.get_hostname())
        self._prof.prof('sub_agent_start', uid=self._pid)

        # expose heartbeat channel to sub-agents, bridges and components,
        # and start those
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cfg.heartbeat = self._cmgr.cfg.heartbeat

        self._cmgr.start_bridges()
        self._cmgr.start_components()

        # at this point the session is up and connected, and it should have
        # brought up all communication bridges and components.  We are
        # ready to rumble!
        rpu.Worker.__init__(self, self._cfg, session)


    # --------------------------------------------------------------------------
    def finalize(self):

        self._prof.prof('sub_agent_stop', uid=self._pid, msg=self._uid)


# ------------------------------------------------------------------------------

