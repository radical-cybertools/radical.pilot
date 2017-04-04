
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import pprint
import signal
import setproctitle

from   .agent_0 import Agent_0
from   .agent_n import Agent_n


# ==============================================================================
#
# Agent bootstrap stage 4
#
# ==============================================================================
def bootstrap_3(agent_name):
    """
    This method continues where the bootstrap_1/2 left off, but will soon pass
    control to the Agent class which will spawn the functional components.
    Before doing so, we will check if we happen to be agent instance zero.  If
    that is the case, some additional python level bootstrap routines kick in,
    to set the stage for component and sub-agent spawning.

    The agent interprets a config file, which will specify in an 'agents'
    section:
      - what nodes should be used for sub-agent startup
      - what bridges should be started
      - what are the endpoints for bridges which are not started
      - what components should be started
    agent_0 will create derived config files for all sub-agents.
    """

    print "bootstrap agent %s" % agent_name
    agent = None

    try:
        setproctitle.setproctitle('rp.%s' % agent_name)

        if agent_name == 'agent_0':
            agent = Agent_0(agent_name)
        else:
            agent = Agent_n(agent_name)

        print 'start   %s' % agent_name
        agent.start(spawn=False)
        print 'started %s' % agent_name

        # we never really quit this way, but instead the agent command_cb may
        # pick up a shutdown signal, the watcher_cb may detect a failing
        # component or sub-agent, or we get a kill signal from the RM.  In all
        # three cases, we'll end up in agent.stop() -- agent.wait() will wait
        # until then.
        agent.join()
        while True:
            time.sleep(1)

    finally:

        # in all cases, make sure we perform an orderly shutdown.  I hope python
        # does not mind doing all those things in a finally clause of
        # (essentially) main...
        print 'finally %s' % agent_name
        if agent:
            print 'finally stop %s' % agent_name
            agent.stop()
            print 'finally join %s' % agent_name
            agent.join()
            print 'finally joined %s' % agent_name

# ------------------------------------------------------------------------------
