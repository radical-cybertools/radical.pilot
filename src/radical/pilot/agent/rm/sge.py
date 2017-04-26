
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

from base import LRMS


# ==============================================================================
#
class SGE(LRMS):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LRMS.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        sge_hostfile = os.environ.get('PE_HOSTFILE')
        if sge_hostfile is None:
            msg = "$PE_HOSTFILE not set!"
            self._log.error(msg)
            raise RuntimeError(msg)

        def _sigusr2_handler():

            self._log.warn('caught sigusr2')
            # self.stop()

            # doing stuff in the signal handler is usually not a great idea.
            # Doing complex stuff like termination even less so.  So for now we
            # hook into the ru.Process class and signal termination gracefully.
            # TODO: provide cleaner hook in RU
            term = getattr(self, '_ru_term')
            if term is not None and not term.is_set():
                term.set()
        signal.signal(signal.SIGUSR1, _sigusr2_handler)

        # SGE core configuration might be different than what multiprocessing
        # announces
        # Alternative: "qconf -sq all.q|awk '/^slots *[0-9]+$/{print $2}'"

        # Parse SGE hostfile for nodes
        sge_node_list = [line.split()[0] for line in open(sge_hostfile)]
        self._log.info("Found PE_HOSTFILE %s. Expanded to: %s", sge_hostfile, sge_node_list)

        # Parse SGE hostfile for cores
        sge_cores_count_list = [int(line.split()[1]) for line in open(sge_hostfile)]
        sge_core_counts = list(set(sge_cores_count_list))

        # Check if nodes have the same core count
        if len(sge_core_counts) == 1:
            sge_cores_per_node = min(sge_core_counts)
            self._log.info("Found unique core counts: %s Using: %d", sge_core_counts, sge_cores_per_node)

            self.node_list = list(set(sge_node_list))
            self.cores_per_node = sge_cores_per_node

        else:
            # In case of non-homogeneous counts, consider all slots be single core
            sge_cores_per_node = 1
            self._log.info("Found unique core counts: %s Using: %d", sge_core_counts, sge_cores_per_node)
            self.cores_per_node = sge_cores_per_node

            # Expand node list
            self.node_list = []
            for x in zip(sge_node_list, sge_cores_count_list):
                self.node_list.extend(x[1] * [x[0]])
