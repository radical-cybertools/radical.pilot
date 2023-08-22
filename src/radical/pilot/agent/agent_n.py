
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import radical.utils  as ru

from .. import utils as rpu

from .. import Session


# ------------------------------------------------------------------------------
#
class Agent_n(rpu.Worker):

    # This is a sub-agent.  It does not do much apart from starting
    # agent components and watching them, which is all taken care of in the
    # `Worker` base class (or rather in the `Component` base class of `Worker`).

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg: ru.Config, session):

        self._cfg      = cfg
        self._sid      = cfg.sid
        self._pid      = cfg.pid
        self._pmgr     = cfg.pmgr
        self._pwd      = cfg.pilot_sandbox
        self._sid      = cfg.sid
        self._reg_addr = cfg.reg_addr

        self._session  = session

        # log / profile via session until component manager is initialized
        self._log     = self._session._log
        self._prof    = self._session._prof

        self._starttime   = time.time()
        self._final_cause = None

        # this is the earliest point to sync bootstrap and agent profiles
        self._prof.prof('hostname', uid=self._pid, msg=ru.get_hostname())
        self._prof.prof('sub_agent_start', uid=self._pid)

        # at this point the session is up and connected, and it should have
        # brought up all communication bridges and components.  We are
        # ready to rumble!
        rpu.Worker.__init__(self, self._cfg, self._session)


    # --------------------------------------------------------------------------
    def finalize(self):

        self._prof.prof('sub_agent_stop', uid=self._pid, msg=self._uid)


# ------------------------------------------------------------------------------

