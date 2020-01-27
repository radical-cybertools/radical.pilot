
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import subprocess
import multiprocessing

from .base import ResourceManager


# ------------------------------------------------------------------------------
#
class Yarn(ResourceManager):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        ResourceManager.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info("Using YARN on localhost.")

        selected_cpus = self.requested_cores

        # when we profile the agent, we fake any number of cores, so don't
        # perform any sanity checks.  Otherwise we use at most all available
        # cores (and informa about unused ones)
        if self._prof.enabled:

            detected_cpus = multiprocessing.cpu_count()

            if detected_cpus < selected_cpus:
                self._log.warn("insufficient cores: using available %d instead of requested %d.",
                        detected_cpus, selected_cpus)
                selected_cpus = detected_cpus

            elif detected_cpus > selected_cpus:
                self._log.warn("more cores available: using requested %d instead of available %d.",
                        selected_cpus, detected_cpus)

        hdfs_conf_output = subprocess.check_output(['hdfs', 'getconf',
                                                    '-nnRpcAddresses']).split('\n')[0]
        self._log.debug('Namenode URL = %s', hdfs_conf_output)
        self.namenode_url = hdfs_conf_output


        self._log.debug('Namenode URL = %s', self.namenode_url)

        # I will leave it for the moment because I have not found another way
        # to take the necessary value yet.
        yarn_conf_output = subprocess.check_output(['yarn', 'node', '-list'],
                                                   stderr=subprocess.STDOUT).split('\n')
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

        hostname = os.environ.get('HOSTNAME')

        if hostname is None:
            self.node_list = ['localhost', 'localhost']
        else:
            self.node_list = [hostname, hostname]

        self.cores_per_node = selected_cpus
        self.gpus_per_node  = self._cfg.get('gpus_per_node', 0)  # FIXME GPU


# ------------------------------------------------------------------------------

