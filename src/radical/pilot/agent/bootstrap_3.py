
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
    This method continues where the bootstrap_1/2 left off, but will soon pass
    control to the Agent class which will spawn the functional components.
    Before doing so, we will check if we happen to be agent instance zero.  If
    that is the case, some additional python level bootstrap routines kick in,
    to set the stage for component and sub-agent spawning.

    The agent interprets a config file, which will specify in an agent_layout
    section:
      - what nodes should be used for sub-agent startup
      - what bridges should be started
      - what are the endpoints for bridges which are not started
      - what components should be started
    agent_0 will create derived config files for all sub-agents.
    """

    agent_0 = None
    try:
        setproctitle.setproctitle('rp.agent_0')

        agent_0 = Agent_0()
        agent_0.start(spawn=False)

        # we never really quit this way, but instead the agent command_cb may
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
