
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils as ru

from ... import utils     as rpu
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RP's spawner types
EXECUTING_NAME_POPEN = "POPEN"
EXECUTING_NAME_SHELL = "SHELL"
EXECUTING_NAME_ABDS  = "ABDS"
EXECUTING_NAME_ORTE  = "ORTE"


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
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.executing.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)

        # if so configured, let the CU know what to use as tmp dir
        self._cu_tmp = cfg.get('cu_tmp', os.environ.get('TMP', '/tmp'))


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg['spawner']

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError("Factory only available to base class!")

        if name == EXECUTING_NAME_POPEN:
            from .popen import Popen
            impl = Popen(cfg, session)
        elif name == EXECUTING_NAME_SHELL:
            from .shell import Shell
            impl = Shell(cfg, session)
        elif name == EXECUTING_NAME_ABDS:
            from .abds import ABDS
            impl = ABDS(cfg, session)
        elif name == EXECUTING_NAME_ORTE:
            from .orte import ORTE
            impl = ORTE(cfg, session)
        else:
            raise ValueError("AgentExecutingComponent '%s' unknown or defunct" % name)

        return impl


# ------------------------------------------------------------------------------
