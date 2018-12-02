
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"



import os
import copy
import pprint
import threading as mt

import radical.utils as ru

from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
import cProfile
cprof = cProfile.Profile()

def cprof_it(func):
    def wrapper(*args, **kwargs):
        retval = cprof.runcall(func, *args, **kwargs)
        return retval
    return wrapper


def dec_all_methods(dec):
    import inspect
    def dectheclass(cls):
        if ru.is_main_thread():
            cprof_env   = os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "")
            cprof_elems = cprof_env.split()
            if "CONTINUOUS" in cprof_elems:
                for name, m in inspect.getmembers(cls, inspect.ismethod):
                    setattr(cls, name, dec(m))
        return cls
    return dectheclass


# ------------------------------------------------------------------------------
#
@dec_all_methods(cprof_it)
class RPPaper(AgentSchedulingComponent):
    '''
    RPPaper: homogeneous bag-of-task sccheduler - ony full nodes are supported,
    one process per node
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.nodes = None

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # FIXME: this should not be overloaded here, but in the base class
    #
    def finalize_child(self):

        cprof_env = os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "")
        if "RPPaper" in cprof_env.split():
            self_thread = mt.current_thread()
            cprof.dump_stats("python-%s.profile" % self_thread.name)

        # make sure that parent finalizers are called
        super(RPPaper, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # NOTE: We delay the actual configuration until we received the first
        #       unit to schedule - at that point we can slice and dice the
        #       resources into suitable static slots.
        self._configured = False

        self.free = list()     # declare for early debug output


    # --------------------------------------------------------------------------
    #
    def _delayed_configure(self, cud):

        if self._configured:
            return

        self.cpn     = self._lrms_cores_per_node
        self.gpn     = self._lrms_gpus_per_node

        self.free    = list()     # list of free chunks
        self.lock    = mt.Lock()  # lock for the above list

        cores_needed = int(cud['cpu_processes']) * int(cud['cpu_threads'])
        gpus_needed  = int(cud['gpu_processes'])

        if int(cud['cpu_threads']) > self.cpn:
                raise ValueError('threads do not fit on node')

        # how many nodes does a unit need?
        cnodes = max(cores_needed / self.cpn, gpus_needed / self.gpn)


        # ----------------------------------------------------------------------
        #
        idx = 0
        while True:
            slots = list()
            for i in range(cnodes):
                node  = self.nodes[idx + i]
                nuid  = node['uid']
                nname = node['name']
                slots.append([nname, nuid, 
                              [list(range(int(cud['cpu_threads'])))], []])

            self.free.append({'nodes'         : slots,
                              'cores_per_node': self.cpn,
                              'gpus_per_node' : self.gpn,
                              'lm_info'       : self._lrms_lm_info, 
                              'ncblocks'      : 0, 
                              'ngblocks'      : 0})

            idx += cnodes
            if idx + cnodes >= len(self.nodes):
                break

        self._configured = True

        pprint.pprint(self.free)


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cud):
        '''
        This is the main method of this implementation, and is triggered when
        a unit needs to be mapped to a set of cores / gpus.
        '''

      # self._log.debug('=> allocate [%d]', len(self.free))
        if not self._configured:
            self._delayed_configure(cud)

      # self._log.debug('find new slot')
        slots = self._find_slots(cud)
      # self._log.debug('allocate slots %s', slots)

      # self._log.debug('<= allocate [%d]', len(self.free))

        return slots


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):
        '''
        This method is called when previously aquired resources are not needed
        anymore.  `slots` are the resource slots as previously returned by
        `_allocate_slots()`.
        '''

      # self._log.debug('=> release  [%d]', len(self.free))
      # self._log.debug('release  slot %s', slots['nodes'])
        with self.lock:
            self.free.append(slots)
      # self._log.debug('<= release  [%d]', len(self.free))


    # --------------------------------------------------------------------------
    #
    def _find_slots(self, cores_requested):

        # check if we have free chunks laying around - return one
        with self.lock:
            if self.free:
                return self.free.pop()
            else:
                return None


# ------------------------------------------------------------------------------

