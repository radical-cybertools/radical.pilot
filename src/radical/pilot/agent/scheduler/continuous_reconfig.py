
__copyright__ = 'Copyright 2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import copy

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
    def initialize(self):

        super().initialize()

        reqs_file = self._rm._cfg.get('config', {}).get('task_reqs')

        if reqs_file and os.path.isfile(reqs_file):
            self._task_reqs.update(ru.read_json(reqs_file))
            self._log.debug('set task reqs: %s', self._task_reqs)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        if not self._task_reqs:
            return super().work(tasks)


        new_tasks = list()
        for task in ru.as_list(tasks):

            if not self._task_reqs:
                new_tasks.append(task)

            else:

                handled = False
                for attr in ['ranks', 'cores_per_rank']:

                    v = int(self._task_reqs.get(attr) or 0)

                    if v:
                        new_task = copy.deepcopy(task)
                        descr = new_task['description']

                        new_descr = copy.deepcopy(descr)
                        new_descr[attr] = v

                        if descr['cores_per_rank'] > 1:
                            descr['threading_type'] = rpc.OpenMP

                        new_tasks.append(new_task)
                        handled = True

                if not handled:
                    new_tasks.append(task)

        super().work(new_tasks)


# ------------------------------------------------------------------------------

