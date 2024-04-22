
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import math
import multiprocessing

import radical.utils as ru

from .base import RMInfo, ResourceManager

from ...pilot_description import PilotDescription


# ------------------------------------------------------------------------------
#
class Fork(ResourceManager):


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _inspect(cls) -> PilotDescription:
        '''
        This method will inspect the local nodea and create a suitable pilot
        description.
        '''

        # this inspect should always return a valid pilot description, as it
        # will be used to create a pilot for the local node.

        out, err, ret = ru.sh_callout('lspci | grep " VGA"',
                                      shell=True)
        if ret: n_gpus = None
        else  : n_gpus = len(out.split('\n'))

        out, err, ret = ru.sh_callout('cat /proc/cpuinfo | grep processor',
                                      shell=True)
        if ret: n_cores = None
        else  : n_cores = len(out.split('\n'))

        out, err, ret = ru.sh_callout('free -m | grep "Mem: "', shell=True)
        if ret: mem = None
        else  : mem = int(out.split()[1])

        out, err, ret = ru.sh_callout('free -m | grep "Mem: "', shell=True)
        if ret: mem = None
        else  : mem = int(out.split()[1])

        out, err, ret = ru.sh_callout('df -m / | grep "/"', shell=True)
        if ret: lfs = None
        else  : lfs = int(out.split()[3])

        resource = 'local.localhost'

        return PilotDescription({'resource': resource,
                                 'nodes'   : 1})


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        detected_cores = multiprocessing.cpu_count()
        if not rm_info.cores_per_node:
            rm_info.cores_per_node = detected_cores

        if self._rcfg.fake_resources:
            self._log.info(
                'virtual resource with %d cores per node (%d detected cores)' %
                (rm_info.cores_per_node, detected_cores))
        elif detected_cores >= rm_info.requested_cores:
            if rm_info.cores_per_node > detected_cores:
                self._log.warn(
                    'defined %d cores per node > %d detected cores' %
                    (rm_info.cores_per_node, detected_cores))
            elif rm_info.cores_per_node < rm_info.requested_cores:
                raise RuntimeError(
                    'incorrectly defined %d cores per node for real resource,'
                    'while requesting %d cores' %
                    (rm_info.cores_per_node, rm_info.requested_cores))
        else:
            raise RuntimeError(
                'insufficient cores found (%d < %d)' %
                (detected_cores, rm_info.requested_cores))

        # duplicates the code from the base class, but here it should be
        # called first before determining `rm_info.node_list`
        if not rm_info.requested_nodes:
            n_nodes = rm_info.requested_cores / rm_info.cores_per_node
            rm_info.requested_nodes = math.ceil(n_nodes)
            # FIXME: number of GPUs should also be checked - how?
            #        Should we use hwlock if available?

        nodes = [('localhost', rm_info.cores_per_node)
                 for _ in range(rm_info.requested_nodes)]

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

