
__copyright__ = 'Copyright 2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from ... import constants as rpc

from .continuous import Continuous


# ------------------------------------------------------------------------------
#
class ContinuousReconfig(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)

        self._task_reqs = {}  # resource requirements (ranks, cores_per_rank)

    # --------------------------------------------------------------------------
    #
    def _set_task_reqs(self):

        reqs_file = ''
        if os.path.isfile(reqs_file):
            self._task_reqs.update(ru.read_json(reqs_file))

    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not self._task_reqs:
            self._set_task_reqs()

        if self._task_reqs:

            tasks_reconfigured = []
            for task in ru.as_list(tasks):
                for attr in ['ranks', 'cores_per_rank']:
                    v = int(self._task_reqs.get(attr) or 0)
                    if v:
                        task[attr] = v
                        tasks_reconfigured.append(task)

                if task['cores_per_rank'] > 1:
                    task['threading_type'] = rpc.OpenMP

            super().work(tasks_reconfigured)

# ------------------------------------------------------------------------------

