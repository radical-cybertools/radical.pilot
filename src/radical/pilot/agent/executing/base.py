
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RP's spawner types
EXECUTING_NAME_POPEN = "POPEN"
EXECUTING_NAME_SHELL = "SHELL"
EXECUTING_NAME_ABDS  = "ABDS"


# ==============================================================================
#
class AgentExecutingComponent(rpu.Component):
    """
    Manage the creation of CU processes, and watch them until they are completed
    (one way or the other).  The spawner thus moves the unit from
    PendingExecution to Executing, and then to a final state (or PendingStageOut
    of course).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        rpu.Component.__init__(self, rpc.AGENT_EXECUTING_COMPONENT, cfg)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg):

        name = cfg['spawner']

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError("Factory only available to base class!")

        from .popen import Popen
        from .shell import Shell
        from .abds  import ABDS


        try:
            impl = {
                EXECUTING_NAME_POPEN : Popen,
                EXECUTING_NAME_SHELL : Shell,
                EXECUTING_NAME_ABDS  : ABDS
            }[name]

            impl = impl(cfg)
            return impl

        except KeyError:
            raise ValueError("AgentExecutingComponent '%s' unknown or defunct" % name)


# ------------------------------------------------------------------------------

