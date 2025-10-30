
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from typing import List, Tuple

import radical.utils as ru

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class PBSPro(ResourceManager):

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def batch_started():

        return bool(os.getenv('PBS_JOBID'))

    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        nodes = None

        try:
            vnodes, rm_info.cores_per_node = self._parse_pbspro_vnodes()
            nodes = [(node, rm_info.cores_per_node) for node in vnodes]

        except (IndexError, ValueError):
            self._log.debug_2('exec_vnodes not detected')

        except RuntimeError as e:
            err_message = str(e)
            if not err_message.startswith('qstat failed'):
                raise
            self._log.debug_1(err_message)

        if not nodes:

            if not rm_info.cores_per_node or 'PBS_NODEFILE' not in os.environ:
                raise RuntimeError('resource configuration unknown, either '
                                   'cores_per_node or $PBS_NODEFILE not set')

            nodes = self._parse_nodefile(os.environ['PBS_NODEFILE'],
                                         cpn=rm_info.cores_per_node,
                                         smt=rm_info.threads_per_core)

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info

    # --------------------------------------------------------------------------
    #
    def _parse_pbspro_vnodes(self) -> Tuple[List[str], int]:

        # PBS Job ID
        jobid = os.environ.get('PBS_JOBID')
        if not jobid:
            raise RuntimeError('$PBS_JOBID not set')

        # Get the output of qstat -f for this job
        output, error, ret = ru.sh_callout(['qstat', '-f', jobid])
        if ret:
            raise RuntimeError('qstat failed: %s' % error)

        # Get the (multiline) 'exec_vnode' entry
        vnodes_str = ''
        for line in output.splitlines():
            line = ru.as_string(line)
            # Detect start of entry
            if 'exec_vnode = ' in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if ' = ' in line:
                    break
                vnodes_str += line.strip()

        # Get the RHS of the entry
        rhs = vnodes_str.split('=', 1)[1].strip()
        self._log.debug_1('exec_vnodes: %s', rhs)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = rhs.find(')+(')
            node_str = rhs[1:idx]
            nodes_list.append(node_str)
            rhs = rhs[idx + 2:]
            if idx < 0:
                break

        vnodes_set = set()
        ncpus_set  = set()
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split('+')
            for _slice in slices:
                vnode, ncpus = _slice.split(':')
                vnodes_set.add(vnode)
                ncpus_set.add(int(ncpus.split('=')[1]))

        self._log.debug_1('vnodes: %s', vnodes_set)
        self._log.debug_1('ncpus: %s',  ncpus_set)

        if len(ncpus_set) > 1:
            raise RuntimeError('detected vnodes of different sizes')

        return sorted(vnodes_set), ncpus_set.pop()


# ------------------------------------------------------------------------------

