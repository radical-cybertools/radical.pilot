
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import pprint
import signal
import setproctitle

import radical.utils  as ru

from .. import worker as rpw
from .. import utils  as rpu


# ==============================================================================
#
# Agent bootstrap stage 4
#
# ==============================================================================
#
# avoid undefined vars on finalization / signal handling
def bootstrap_4(agent_name):
    """
    This method continues where the bootstrapper left off, but will soon pass
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
    bootstrap_3 will create derived config files for all sub-agents.
    """

    try:
        assert(agent_name != 'agent_0')

        print "startup agent %s" % agent_name
        setproctitle.setproctitle('rp.%s' % agent_name)

        # load the agent config, and overload the config dicts
        agent      = None
        agent_cfg  = "%s/%s.cfg" % (os.getcwd(), agent_name)
        cfg        = ru.read_json_str(agent_cfg)
        pilot_id   = cfg['pilot_id']
        session_id = cfg['session_id']

        # set up a logger and profiler
        print "Agent config (%s):\n%s\n\n" % (agent_cfg, pprint.pformat(cfg))

        # des Pudels Kern
        agent = rpw.Agent(cfg)
        agent.start()
        agent.join()


    except SystemExit:
        print "Exit running %s" % agent_name

    except Exception as e:
        print "Error running %s" % agent_name

    finally:

        # in all cases, make sure we perform an orderly shutdown.  I hope python
        # does not mind doing all those things in a finally clause of
        # (essentially) main...
        if agent:
            agent.stop()

        print '%s finalized' % agent_name


# ------------------------------------------------------------------------------

