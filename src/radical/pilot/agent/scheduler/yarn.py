
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
class Yarn(AgentSchedulingComponent):

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

        self._log.info('Checking rm_ip %s' % self._cfg['lrms_info']['lm_info']['rm_ip'])
        self._rm_ip = self._cfg['lrms_info']['lm_info']['rm_ip']
        self._service_url = self._cfg['lrms_info']['lm_info']['service_url']
        self._rm_url = self._cfg['lrms_info']['lm_info']['rm_url']
        self._client_node = self._cfg['lrms_info']['lm_info']['nodename']

        sample_time = rpu.timestamp()
        yarn_status = ul.urlopen('http://{0}:8088/ws/v1/cluster/scheduler'.format(self._rm_ip))

        yarn_schedul_json = json.loads(yarn_status.read())

        max_num_app = yarn_schedul_json['scheduler']['schedulerInfo']['queues']['queue'][0]['maxApplications']
        num_app = yarn_schedul_json['scheduler']['schedulerInfo']['queues']['queue'][0]['numApplications']

        #-----------------------------------------------------------------------
        # Find out the cluster's resources
        cluster_metrics = ul.urlopen('http://{0}:8088/ws/v1/cluster/metrics'.format(self._rm_ip))

        metrics = json.loads(cluster_metrics.read())
        self._mnum_of_cores = metrics['clusterMetrics']['totalVirtualCores']
        self._mmem_size = metrics['clusterMetrics']['totalMB']
        self._num_of_cores = metrics['clusterMetrics']['allocatedVirtualCores']
        self._mem_size = metrics['clusterMetrics']['allocatedMB']

        self.avail_app = {'apps':max_num_app - num_app,'timestamp':sample_time}
        self.avail_cores = self._mnum_of_cores - self._num_of_cores
        self.avail_mem = self._mmem_size - self._mem_size

    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        """
        Finds how many spots are left free in the YARN scheduler queue and also
        updates if it is needed..
        """
        #-------------------------------------------------------------------------
        # As it seems this part of the Scheduler is not according to the assumptions
        # made about slot status. Keeping the code commented just in case it is
        # needed later either as whole or art of it.
        sample = rpu.timestamp()
        yarn_status = ul.urlopen('http://{0}:8088/ws/v1/cluster/scheduler'.format(self._rm_ip))
        yarn_schedul_json = json.loads(yarn_status.read())

        max_num_app = yarn_schedul_json['scheduler']['schedulerInfo']['queues']['queue'][0]['maxApplications']
        num_app = yarn_schedul_json['scheduler']['schedulerInfo']['queues']['queue'][0]['numApplications']
        if (self.avail_app['timestamp'] - sample)>60 and \
           (self.avail_app['apps'] != max_num_app - num_app):
            self.avail_app['apps'] = max_num_app - num_app
            self.avail_app['timestamp']=sample

        return '{0} applications per user remaining. Free cores {1} Free Mem {2}'\
        .format(self.avail_app['apps'],self.avail_cores,self.avail_mem)


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cores_requested,mem_requested):
        """
        In this implementation it checks if the number of cores and memory size
        that exist in the YARN cluster are enough for an application to fit in it.
        """

        #-----------------------------------------------------------------------
        # If the application requests resources that exist in the cluster, not
        # necessarily free, then it returns true else it returns false
        #TODO: Add provision for memory request
        if (cores_requested+1) <= self.avail_cores and \
              mem_requested<=self.avail_mem and \
              self.avail_app['apps'] != 0:
            self.avail_cores -=cores_requested
            self.avail_mem -=mem_requested
            self.avail_app['apps']-=1
            return True
        else:
            return False


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slot):
        #-----------------------------------------------------------------------
        # One application has finished, increase the number of available slots.
        #with self._slot_lock:
        self._log.info('Releasing : {0} Cores, {1} RAM'.format(opaque_slot['task_slots'][0],opaque_slot['task_slots'][1]))
        self.avail_cores +=opaque_slot['task_slots'][0]
        self.avail_mem +=opaque_slot['task_slots'][1]
        self.avail_app['apps']+=1
        return True



    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, cu):
        """
        Attempt to allocate cores for a specific CU.  If it succeeds, send the
        CU off to the ExecutionWorker.
        """
        #-----------------------------------------------------------------------
        # Check if the YARN scheduler queue has space to accept new CUs.
        # Check about racing conditions in the case that you allowed an
        # application to start executing and before the statistics in yarn have
        # refreshed, to send another one that does not fit.

        # TODO: Allocation should be based on the minimum memor allocation per
        # container. Each YARN application needs two containers, one for the
        # Application Master and one for the Container that will run.

        # needs to be locked as we try to acquire slots, but slots are freed
        # in a different thread.  But we keep the lock duration short...
        with self._slot_lock :

            self._log.info(self.slot_status())
            self._log.debug('YARN Service and RM URLs: {0} - {1}'.format(self._service_url,self._rm_url))

            # We also need the minimum memory of the YARN cluster. This is because
            # Java issues a JVM out of memory error when the YARN scheduler cannot
            # accept. It needs to go either from the configuration file or find a
            # way to take this value for the YARN scheduler config.

            cu['opaque_slots']={'lm_info':{'service_url':self._service_url,
                                            'rm_url':self._rm_url,
                                            'nodename':self._client_node},
                                'task_slots':[cu['description']['cores'],2048]
                                            }

            alloc = self._allocate_slot(cu['description']['cores'],2048)

        if not alloc:
            return False

        # got an allocation, go off and launch the process
        self._prof.prof('schedule', msg="allocated", uid=cu['_id'])
        self._log.info("slot status after allocated  : %s" % self.slot_status ())

        return True

    # --------------------------------------------------------------------------
    #
    def work(self, cu):

      # self.advance(cu, rps.AGENT_SCHEDULING, publish=True, push=False)
        self._log.info("Overiding Parent's class method")
        self.advance(cu, rps.ALLOCATING , publish=True, push=False)

        # we got a new unit to schedule.  Either we can place it
        # straight away and move it to execution, or we have to
        # put it on the wait queue.
        if self._try_allocation(cu):
            self._prof.prof('schedule', msg="allocation succeeded", uid=cu['_id'])
            self.advance(cu, rps.EXECUTING_PENDING, publish=False, push=True)

        else:
            # No resources available, put in wait queue
            self._prof.prof('schedule', msg="allocation failed", uid=cu['_id'])
            with self._wait_lock :
                self._wait_pool.append(cu)


# ------------------------------------------------------------------------------

