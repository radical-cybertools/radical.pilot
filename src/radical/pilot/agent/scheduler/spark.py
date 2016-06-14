
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import json
import urllib2 as ul

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentSchedulingComponent


#===============================================================================
#
class Spark(AgentSchedulingComponent):

    # FIXME: clarify what can be overloaded by Scheduler classes

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg):

        AgentSchedulingComponent.__init__(self, cfg)

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        #-----------------------------------------------------------------------
        # Find out how many applications you can submit to YARN. And also keep
        # this check happened to update it accordingly


        #if 'rm_ip' not in self._cfg['lrms_info']:
        #    raise RuntimeError('rm_ip not in lm_info for %s' \
        #            % (self.name))

        self._log.info('Checking rm_ip %s',self._cfg['lrms_info']['lm_info']['rm_ip'])
        self._rm_ip = self._cfg['lrms_info']['lm_info']['rm_ip']
        self._rm_url = self._cfg['lrms_info']['lm_info']['rm_url']
        self._client_node = self._cfg['lrms_info']['lm_info']['nodename']

        sample_time = rpu.timestamp()

        #-----------------------------------------------------------------------
        # Find out the cluster's resources

        self._mnum_of_cores = metrics['clusterMetrics']['totalVirtualCores']
        self._mmem_size = metrics['clusterMetrics']['totalMB']
        self._num_of_cores = metrics['clusterMetrics']['allocatedVirtualCores']
        self._mem_size = metrics['clusterMetrics']['allocatedMB']

        self.avail_app = {'apps':max_num_app - num_app,'timestamp':sample_time}
        self.avail_cores = self._mnum_of_cores - self._num_of_cores
        self.avail_mem = self._mmem_size - self._mem_size


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slot):
        #-----------------------------------------------------------------------
        # One application has finished, increase the number of available slots.
        #with self._slot_lock:
        self._log.info('Releasing : %s Cores, %s RAM',[opaque_slot['task_slots'][0],opaque_slot['task_slots'][1])
        self.avail_cores +=opaque_slot['task_slots'][0]
        self.avail_mem +=opaque_slot['task_slots'][1]
        self.avail_app['apps']+=1
        return True

    # --------------------------------------------------------------------------
    #
    def work(self, cu):

      # self.advance(cu, rp.AGENT_SCHEDULING, publish=True, push=False)
        self._log.info("Overiding Parent's class method")
        self.advance(cu, rp.ALLOCATING , publish=True, push=False)

        # we got a new unit to schedule.  Either we can place it
        # straight away and move it to execution, or we have to
        # put it on the wait queue.
        if self._try_allocation(cu):
            self._prof.prof('schedule', msg="allocation succeeded", uid=cu['_id'])
            self.advance(cu, rp.EXECUTING_PENDING, publish=False, push=True)

        else:
            # No resources available, put in wait queue
            self._prof.prof('schedule', msg="allocation failed", uid=cu['_id'])
            with self._wait_lock :
                self._wait_pool.append(cu)