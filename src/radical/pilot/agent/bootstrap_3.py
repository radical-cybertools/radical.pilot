
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import pprint
import signal
import setproctitle

from   .agent_0 import Agent_0


# ------------------------------------------------------------------------------
#
def bootstrap_3():
    """
    This is only executed by agent_0
    """

    agent_0 = None
    try:
        setproctitle.setproctitle('rp.agent_0')

        agent_0 = Agent_0()
        agent_0.start()

        # we never really quit this way, but instead the agent_0 command_cb may
        # pick up a shutdown signal, the watcher_cb may detect a failing
        # component or sub-agent, or we get a kill signal from the RM.  In all
        # three cases, we'll end up in agent_0.stop()
        while True:
            time.sleep(1)

    finally:

        # in all cases, make sure we perform an orderly shutdown.  I hope python
        # does not mind doing all those things in a finally clause of
        # (essentially) main...
        if agent_0:
            agent_0.stop()


# ------------------------------------------------------------------------------
