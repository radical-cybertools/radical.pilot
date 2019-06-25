
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import multiprocessing

from base import LRMS


# ==============================================================================
#
class Spark(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):
        LRMS.__init__(self, cfg, logger)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info("Using SPARK on localhost.")

        selected_cpus = self.requested_cores

        # when we profile the agent, we fake any number of cores, so don't
        # perform any sanity checks.  Otherwise we use at most all available
        # cores (and informa about unused ones)
        if 'RADICAL_PILOT_PROFILE' not in os.environ:

            detected_cpus = multiprocessing.cpu_count()

            if detected_cpus < selected_cpus:
                self._log.warn("insufficient cores: using available %d instead of requested %d.",
                        detected_cpus, selected_cpus)
                selected_cpus = detected_cpus

            elif detected_cpus > selected_cpus:
                self._log.warn("more cores available: using requested %d instead of available %d.",
                        selected_cpus, detected_cpus)

        hostname = os.environ.get('HOSTNAME')

        if not hostname:
            self.node_list = ['localhost', 'localhost']
        else:
            self.node_list = [hostname, hostname]

        self.cores_per_node = selected_cpus
        self.gpus_per_node  = self._cfg.get('gpus_per_node', 0)  # FIXME GPU


# ------------------------------------------------------------------------------

