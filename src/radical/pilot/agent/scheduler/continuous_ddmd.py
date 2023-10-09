
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import copy

from collections import defaultdict

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

        self._task_types      = defaultdict(list)
        self._deprecated_tags = list()

        self.register_rpc_handler('ddmd_deprecate', self._rpc_deprecate,
                                  addr=self.session.cfg.pid)
        self.register_rpc_handler('ddmd_undeprecate', self._rpc_deprecate,
                                  addr=self.session.cfg.pid)


    # --------------------------------------------------------------------------
    #
    def _rpc_deprecate(self, task_type):

        self._log.debug('=== deprecate task type %s', task_type)

        self._deprecated_tags.append(task_type)

        # cancel all known tasks of that type
        to_cancel = self._task_types[task_type]

        if to_cancel:
            # cancel via control message so that also tasks are being cancelled
            # which are currently executing etc.
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'cancel_tasks',
                                              'arg': {'uids': to_cancel}})


    # --------------------------------------------------------------------------
    #
    def _filter_incoming(self, tasks):
        '''
        Ignore all task types marked as deprecated, and at the same time
        populate the task_type dictionary
        '''

        to_handle = list()
        to_cancel = list()
        for task in tasks:


            task_type = task['description'].get('metadata', {}).get('task_type')

            if task_type and task_type in self._deprecated_tags:
                to_cancel.append(task)

            else:
                if task_type:
                    self._task_types[task_type].append(task['uid'])

                to_handle.append(task)

        if to_cancel:
            self.advance(to_cancel, rps.CANCELED,
                         push=False, publish=True, fwd=True)

        return to_handle


# ------------------------------------------------------------------------------

