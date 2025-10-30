
__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from .base import RMInfo, ResourceManager


# ------------------------------------------------------------------------------
#
class Torque(ResourceManager):

    # --------------------------------------------------------------------------
    #
    @staticmethod
    def batch_started():

        return bool(os.getenv('PBS_JOBID'))


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def _inspect() -> PilotDescription:
        '''
        This method will inspect the local environment and derive a pilot
        description, if possible.
        '''

        hostname = os.environ['LMOD_SYSTEM_NAME']
        nodelist = os.environ['PBS_NODEFILE']
        n_nodes  = 0

        with open(nodelist, 'r') as fin:
            for line in fin:
                line = line.strip()
                if line:
                    n_nodes += 1

        # we now have the hostname, but we need to know the resource label
        resource = None
        sites    = ru.Config('radical.pilot.resource', name='*', expand=False)
        for site in sites:
            if resource:
                break
            for res in sorted(sites[site].keys()):
                if hostname in res:
                    resource = '%s.%s' % (site, res)
                    break

        if not resource:
            raise RuntimeError('hostname %s not in resource config' % hostname)

        if not n_nodes:
            raise RuntimeError('PBS_NODEFILE not usable')

        return PilotDescription(resource=resource, nodes=n_nodes)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        nodefile = os.environ.get('PBS_NODEFILE')
        if not nodefile:
            raise RuntimeError('$PBS_NODEFILE not set')

        nodes = self._parse_nodefile(nodefile)

        if not rm_info.cores_per_node:
            rm_info.cores_per_node = self._get_cores_per_node(nodes)

        rm_info.node_list = self._get_node_list(nodes, rm_info)

        return rm_info


# ------------------------------------------------------------------------------

