
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import multiprocessing
import os
import subprocess

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Yarn(ResourceManager):

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
                               'instead of requested %d.',
                               detected_cpus, info.requested_cores)
                info.requested_cores = detected_cpus

            elif detected_cpus > info.requested_cores:
                self._log.warn('more cores available: using requested %d ' +
                               'instead of available %d.',
                               info.requested_cores, detected_cpus)

        self.namenode_url = subprocess.check_output(
            ['hdfs', 'getconf', '-nnRpcAddresses']).split('\n')[0]
        self._log.debug('Namenode URL = %s', self.namenode_url)

        # I will leave it for the moment because I have not found another way
        # to take the necessary value yet.
        yarn_conf_output = subprocess.check_output(
            ['yarn', 'node', '-list'], stderr=subprocess.STDOUT).split('\n')
        for line in yarn_conf_output:
            if 'ResourceManager' in line:
                settings = line.split('at ')[1]
                if '/' in settings:
                    rm_url = settings.split('/')[1]
                    self.rm_ip = rm_url.split(':')[0]
                    self.rm_port = rm_url.split(':')[1]
                else:
                    self.rm_ip = settings.split(':')[0]
                    self.rm_port = settings.split(':')[1]

        info.cores_per_node = info.requested_cores
        info.node_list = [os.environ.get('HOSTNAME') or 'localhost', '1']

        return info

# ------------------------------------------------------------------------------

