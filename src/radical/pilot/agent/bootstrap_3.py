
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
def bootstrap_3(agent_name):
    """
    This is only executed by agent.P.0 for each partision P

    """

    print "bootstrap agent %s" % agent_name
    agent = None

    try:
        setproctitle.setproctitle('rp.%s' % agent_name)

        agent = Agent_0(agent_name)

        print 'start   %s' % agent_name
        agent.start(spawn=False)
        print 'started %s' % agent_name

        # we never really quit here, but instead the agent command_cb may
        # pick up a shutdown signal, the watcher_cb may detect a failing
        # component or sub-agent, or we get a kill signal from the RM.  In all
        # three cases, we'll end up in agent.stop().
        while True:
            print 'xxx'
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

