
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import copy

from .continuous import Continuous

from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
#
class ContinuousDDMD(Continuous):

    # --------------------------------------------------------------------------
    #
    def _configure_scheduler_process(self):

        Continuous._configure_scheduler_process(self)

        self._task_types = dict()

        self.register_publisher(rpc.CONTROL_PUBSUB)
        self.register_rpc_handler('ddmd_deprecate', self._rpc_deprecate,
                                  addr=self.session.cfg.pid)


    # --------------------------------------------------------------------------
    #
    def _rpc_deprecate(self, task_type):

        self._log.debug('=== deprecate task type %s', task_type)


# ------------------------------------------------------------------------------

