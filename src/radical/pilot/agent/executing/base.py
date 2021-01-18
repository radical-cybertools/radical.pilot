
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils as ru

from ... import utils as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's spawner types
EXECUTING_NAME_POPEN   = "POPEN"
EXECUTING_NAME_SHELL   = "SHELL"
EXECUTING_NAME_SHELLFS = "SHELLFS"
EXECUTING_NAME_FLUX    = "FLUX"
EXECUTING_NAME_SLEEP   = "SLEEP"
EXECUTING_NAME_FUNCS   = "FUNCS"

# archived
#
# EXECUTING_NAME_ABDS    = "ABDS"
# EXECUTING_NAME_ORTE    = "ORTE"


# ------------------------------------------------------------------------------
#
class AgentExecutingComponent(rpu.Component):
    """
    Manage the creation of Task processes, and watch them until they are completed
    (one way or the other).  The spawner thus moves the task from
    PendingExecution to Executing, and then to a final state (or PendingStageOut
    of course).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.executing.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)

        # if so configured, let the Task know what to use as tmp dir
        self._task_tmp = cfg.get('task_tmp', os.environ.get('TMP', '/tmp'))


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

        from .popen    import Popen
        from .shell    import Shell
        from .shell_fs import ShellFS
        from .flux     import Flux
        from .funcs    import FUNCS
        from .sleep    import Sleep

      # from .abds     import ABDS
      # from .orte     import ORTE

        try:
            impl = {
                EXECUTING_NAME_POPEN  : Popen,
                EXECUTING_NAME_SHELL  : Shell,
                EXECUTING_NAME_SHELLFS: ShellFS,
                EXECUTING_NAME_FLUX   : Flux,
                EXECUTING_NAME_SLEEP  : Sleep,
                EXECUTING_NAME_FUNCS  : FUNCS,
              # EXECUTING_NAME_ABDS   : ABDS,
              # EXECUTING_NAME_ORTE   : ORTE,
            }[name]
            return impl(cfg, session)

        except KeyError as e:
            raise RuntimeError("AgentExecutingComponent '%s' unknown" % name) \
                from e


# ------------------------------------------------------------------------------

