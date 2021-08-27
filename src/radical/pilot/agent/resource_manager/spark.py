
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import multiprocessing
import os

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Spark(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _update_info(self, info):

        # when we profile the agent, we fake any number of cores, so don't
        # perform any sanity checks.  Otherwise we use at most all available
        # cores (and inform about unused ones)
        if self._prof.enabled:

            detected_cpus = multiprocessing.cpu_count()

            if detected_cpus < info.requested_cores:
                self._log.warn('insufficient cores: using available %d ' +
                               'instead of requested %d',
                               detected_cpus, info.requested_cores)
                info.requested_cores = detected_cpus

            elif detected_cpus > info.requested_cores:
                self._log.warn('more cores available: using requested %d ' +
                               'instead of available %d',
                               info.requested_cores, detected_cpus)

        info.cores_per_node = info.requested_cores
        info.node_list = [os.environ.get('HOSTNAME') or 'localhost', '1']

        return info

# ------------------------------------------------------------------------------

