
__copyright__ = 'Copyright 2023-2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .continuous import Continuous


# ------------------------------------------------------------------------------
#
class ContinuousReconfig(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)

        # file with updated resource requirements (ranks, cores_per_rank)
        self._reconfig_src = self._cfg.reconfig_src
        self._task_reqs    = {}

    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        # check possibly updated task requirements
        if self._reconfig_src and os.path.isfile(self._reconfig_src):
            task_reqs = ru.read_json(self._reconfig_src)
            if self._task_reqs != task_reqs:
                self._log.debug('set new task reqs: %s', task_reqs)
                self._task_reqs = task_reqs

        if not self._task_reqs:
            return super().work(tasks)

        tasks_updated = ru.as_list(tasks)
        for attr in ['ranks', 'cores_per_rank']:
            v = self._task_reqs.get(attr)
            if v is not None:
                v = int(v)
                for task in tasks_updated:
                    task['description'][attr] = v

        super().work(tasks_updated)


# ------------------------------------------------------------------------------

