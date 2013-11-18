#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import os
import psutil
import hostlist

from radical.utils import which
from constants import *

LOG_SUFFIX = 'env'

#-----------------------------------------------------------------------------
#
class ExecutionEnvironment(object):

    #-------------------------------------------------------------------------
    #
    @classmethod
    def discover(cls, logger=None):

        eenv = cls(logger)
        # detect nodes, cores and memory available
        eenv._detect_nodes()
        eenv._detect_cores_and_memory()

        # check for 'mpirun'
        eenv._mpirun_location = which('mpirun')
        eenv._aprun_location  = which('aprun')
        eenv._ssh_location    = which('ssh')

        # suggest a launch method. the current precendce is 
        # aprun, mpirun, ssh, fork. this can be overrdden 
        # by passing the '--launch-method' parameter to the agent.

        if eenv._aprun_location is not None:
            eenv._launch_method = LAUNCH_METHOD_APRUN
        elif eenv._mpirun_location is not None:
            eenv._launch_method = LAUNCH_METHOD_MPIRUN
        elif eenv._ssh_location is not None:
            eenv._launch_method = LAUNCH_METHOD_SSH
        else:
            eenv._launch_method = LAUNCH_METHOD_LOCAL

        # create node dictionary
        for rn in eenv._raw_nodes:
            if rn not in eenv._nodes:
                eenv._nodes[rn] = {#'_count': 1,
                                   'cores': eenv._cores_per_node,
                                   'memory': eenv._memory_per_node}
            #else:
            #    eenv._nodes[rn]['_count'] += 1

        if logger is not None:
            logger.info(
                message="Discovered execution environment: %s" % eenv._nodes,
                suffix=LOG_SUFFIX)

        return eenv

    #-------------------------------------------------------------------------
    #
    def __init__(self, logger=None):
        '''le constructeur
        '''
        self.log = logger

        self._nodes = dict()
        self._raw_nodes = list()
        self._cores_per_node = 0
        self._memory_per_node = 0
        self._launch_method = None

        self._aprun_location  = None
        self._mpirun_location = None
        self._ssh_location    = None

    #-------------------------------------------------------------------------
    #
    @property
    def raw_nodes(self):
        return self._raw_nodes

    #-------------------------------------------------------------------------
    #
    @property
    def nodes(self):
        return self._nodes

    #-------------------------------------------------------------------------
    #
    @property
    def launch_method(self):
        return self._launch_method

    #-------------------------------------------------------------------------
    #
    @property
    def mpirun(self):
        if self._mpirun_location is None:
            return 'mpirun'
        else:
            return self._mpirun_location

    #-------------------------------------------------------------------------
    #
    @property
    def aprun(self): 
        if self._aprun_location is None:
            return 'aprun'
        else:
            return self._aprun_location

    #-------------------------------------------------------------------------
    #
    @property
    def ssh(self):
        if self._ssh_location is None:
            return 'ssh'
        else:
            return self._ssh_location

    #-------------------------------------------------------------------------
    #
    def _detect_cores_and_memory(self):
        self._cores_per_node = psutil.NUM_CPUS
        mem_in_megabyte = int(psutil.virtual_memory().total/1024/1024)
        self._memory_per_node = mem_in_megabyte

    #-------------------------------------------------------------------------
    #
    def _detect_nodes(self):
        # see if we have a PBS_NODEFILE
        pbs_nodefile = os.environ.get('PBS_NODEFILE')
        slurm_nodelist = os.environ.get('SLURM_NODELIST')

        if pbs_nodefile is not None:
            # parse PBS the nodefile
            self._raw_nodes = [line.strip() for line in open(pbs_nodefile)]
            if self.log is not None:
                self.log.info(
                    message="Found PBS_NODEFILE %s: %s" % (pbs_nodefile, self._raw_nodes),
                    suffix=LOG_SUFFIX)

        elif slurm_nodelist is not None:
            # parse SLURM nodefile
            self._raw_nodes = hostlist.expand_hostlist(slurm_nodelist)
            if self.log is not None:
                self.log.info(
                    message="Found SLURM_NODELIST %s. Expanded to: %s" % (slurm_nodelist, self._raw_nodes),
                    suffix=LOG_SUFFIX)

        else:
            self._raw_nodes = ['localhost']

            if self.log is not None:
                self.log.info(
                    message="No PBS_NODEFILE or SLURM_NODELIST found. Using hosts: %s" % (self._raw_nodes),
                    suffix=LOG_SUFFIX)
