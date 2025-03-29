
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import subprocess

import radical.utils as ru

from .base import RMInfo
from .fork import Fork


# ------------------------------------------------------------------------------
#
class Yarn(Fork):

    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, rm_info: RMInfo) -> RMInfo:

        # initialize nodelist from FORK RM
        super().init_from_scratch(rm_info)

        self.namenode_url = subprocess.check_output(
            ['hdfs', 'getconf', '-nnRpcAddresses']).split(b'\n')[0]
        self._log.debug('Namenode URL = %s', self.namenode_url)

        # I will leave it for the moment because I have not found another way
        # to take the necessary value yet.
        yarn_conf_output = subprocess.check_output(
            ['yarn', 'node', '-list'], stderr=subprocess.STDOUT).split(b'\n')
        for line in yarn_conf_output:
            line = ru.as_string(line)
            if 'ResourceManager' in line:
                settings = line.split('at ')[1]
                if '/' in settings:
                    rm_url = settings.split('/')[1]
                    self.rm_ip = rm_url.split(':')[0]
                    self.rm_port = rm_url.split(':')[1]
                else:
                    self.rm_ip = settings.split(':')[0]
                    self.rm_port = settings.split(':')[1]

        return rm_info


# ------------------------------------------------------------------------------

