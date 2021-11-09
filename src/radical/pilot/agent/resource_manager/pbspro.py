
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import subprocess

from typing import List

import radical.utils as ru

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class PBSPro(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        # TODO: $NCPUS?!?! = 1 on archer

        cpn = os.environ.get('NUM_PPN') or os.environ.get('SAGA_PPN')
        if not cpn:
            raise RuntimeError('$NUM_PPN and $SAGA_PPN not set!')

        pbspro_vnodes = self._parse_pbspro_vnodes()

        nodes = [(name, int(cpn)) for name in sorted(pbspro_vnodes)]

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


    # --------------------------------------------------------------------------
    #
    def _parse_pbspro_vnodes(self) -> List[str]:

        # PBS Job ID
        pbspro_jobid = os.environ.get('PBS_JOBID')
        if not pbspro_jobid:
            raise RuntimeError('$PBS_JOBID not set')

        # Get the output of qstat -f for this job
        output = subprocess.check_output(['qstat', '-f', pbspro_jobid])

        # Get the (multiline) 'exec_vnode' entry
        vnodes_str = ''
        for line in output.splitlines():
            line = ru.as_string(line)
            # Detect start of entry
            if 'exec_vnode = ' in line:
                vnodes_str += line.strip()
            elif vnodes_str:
                # Find continuing lines
                if ' = ' not in line:
                    vnodes_str += line.strip()
                else:
                    break

        # Get the RHS of the entry
        rhs = vnodes_str.split('=', 1)[1].strip()
        self._log.debug('input: %s', rhs)

        nodes_list = []
        # Break up the individual node partitions into vnode slices
        while True:
            idx = rhs.find(')+(')

            node_str = rhs[1:idx]
            nodes_list.append(node_str)
            rhs = rhs[idx + 2:]

            if idx < 0:
                break

        vnodes_list = []
        cpus_list = []
        # Split out the slices into vnode name and cpu count
        for node_str in nodes_list:
            slices = node_str.split('+')
            for _slice in slices:
                vnode, cpus = _slice.split(':')
                cpus = int(cpus.split('=')[1])
                self._log.debug('vnode: %s cpus: %s', vnode, cpus)
                vnodes_list.append(vnode)
                cpus_list.append(cpus)

        self._log.debug('vnodes: %s', vnodes_list)
        self._log.debug('cpus: %s', cpus_list)

        cpus_list = list(set(cpus_list))
        min_cpus = int(min(cpus_list))

        if len(cpus_list) > 1:
            self._log.debug('Detected vnodes of different sizes: %s, ' +
                            'the minimal is: %d.', cpus_list, min_cpus)

        node_list = []
        for vnode in vnodes_list:
            node_list.append(vnode)

        # only unique node names
        node_list = list(set(node_list))
        self._log.debug('Node list: %s', node_list)

        # Return the list of node names
        return sorted(node_list)


# ------------------------------------------------------------------------------

