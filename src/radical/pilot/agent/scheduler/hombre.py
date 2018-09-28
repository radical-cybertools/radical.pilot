
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"



import os
import pprint
import threading as mt

import radical.utils as ru

from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
# FIXME: make this a runtime switch depending on cprofile availability
# FIXME: move this to utils (implies another parameter to `dec_all_methods()`)
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
class Hombre(AgentSchedulingComponent):
    '''
    HOMBRE: HOMogeneous Bag-of-task REsource allocator.  Don't kill me...

    The scheduler implementation manages a set of resources (cores).  When it
    gets the first scheduling request, it will assume that to be representative
    *for all future sccheduling requests*, ie. it assumes that all tasks are
    homogeneous.  It thus splits the resources into equal sized chunks and
    assigns them to the incoming requests.  The chunking is fast, no decisions
    need to be made, the scheduler really just hands out pre-defined chunks,
    which is relatively quick - that's the point, to trade generality with
    performance.
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
        if "HOMBRE" in cprof_env.split():
            self_thread = mt.current_thread()
            cprof.dump_stats("python-%s.profile" % self_thread.name)

        # make sure that parent finalizers are called
        super(Hombre, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # * oversubscribe:
        #   Cray's aprun for example does not allow us to oversubscribe CPU
        #   cores on a node, so we can't, say, run n CPU processes on an n-core
        #   node, and than add one additional process for a GPU application.  If
        #   `oversubscribe` is set to False (which is the default for now),
        #   we'll prevent that behavior by allocating one additional CPU core
        #   for each set of requested GPU processes.
        self._oversubscribe = self._cfg.get('oversubscribe', True)

        # NOTE: We delay the actual configuration until we received the first
        #       unit to schedule - at that point we can slice and dice the
        #       resources into suitable static slots.
        self._configured = False


    # --------------------------------------------------------------------------
    #
    def _delayed_configure(self, cud):

        if self._configured:
            return

        self.chunk  = {'cpu_processes'    : cud['cpu_processes'   ],
                       'cpu_process_type' : cud['cpu_process_type'],
                       'cpu_threads'      : cud['cpu_threads'     ],
                       'cpu_thread_type'  : cud['cpu_thread_type' ],

                       'gpu_processes'    : cud['gpu_processes'   ],
                       'gpu_process_type' : cud['gpu_process_type'],
                       'gpu_threads'      : cud['gpu_threads'     ],
                       'gpu_thread_type'  : cud['gpu_thread_type' ],
                       }

        self.cpn     = self._lrms_cores_per_node
        self.gpn     = self._lrms_gpus_per_node

        self.free    = list()     # list of free chunks
        self.lock    = mt.Lock()  # lock for the above list

        cores_needed = cud['cpu_processes'] * cud['cpu_threads']
        gpus_needed  = cud['gpu_processes']

        # check if we need single or multi-node chunks
        single_node = False
        if  cud['cpu_process_type'] != 'MPI' and \
            cud['gpu_process_type'] != 'MPI' :

            single_node = True

        # ---------------------------------------------------------------------
        # create as many equal sized chunks from the available nodes as
        # possible, and put them into the `free` list.  The actual scheduling
        # algorithm will blindly pick chunks from that list whenever a new CUD
        # arrives.
        if single_node:

            # how many CUs fit on a single node?
            # NOTE: this relies on integer divide being floored
            if self._oversubscribe:
                units_per_node = min(self.cpn / cores_needed, 
                                     self.gpn / gpus_needed )
            else:
                units_per_node = min(self.cpn / (cores_needed + gpus_needed), 
                                     self.gpn / (gpus_needed               ))

            assert(units_per_node), 'Non-mpi unit does not fit onto single node'
          # print 'upn: %d' % units_per_node

            for node in self.nodes:

                node_uid  = node['uid']
                node_name = node['name']

                core_idx  = 0
                gpu_idx   = 0

                # alloc chunks on this node for all units it can host
                for _ in range(units_per_node):

                    core_map = list()
                    gpu_map  = list()

                    for _ in range(cud['cpu_processes']):
                        tmp = list()
                        for _ in range(cud['cpu_threads']):
                          # print 'cpu %d' % core_idx,
                            tmp.append(core_idx)
                            core_idx += 1
                        core_map.append(tmp)

                    # account for the gpu process cores, if needed
                    if not self._oversubscribe:
                        core_idx += cud['gpu_processes']

                    for _ in range(cud['gpu_processes']):
                        tmp = list()
                        for _ in range(cud['gpu_threads']):
                          # print 'gpu %d' % gpu_idx
                            tmp.append(gpu_idx)
                            gpu_idx += 1
                        gpu_map.append(tmp)

                    slots = {'nodes'         : [[node_name, node_uid, core_map, gpu_map]],
                             'cores_per_node': self.cpn,
                             'gpus_per_node' : self.gpn,
                             'lm_info'       : self._lrms_lm_info
                             }
                    self.free.append(slots)
                  # print '-->'
                  # pprint.pprint(slots)
                  # print

                assert(core_idx <= self.cpn), 'inconsistent scheduler state'
                assert(gpu_idx  <= self.gpn), 'inconsistent scheduler state'

          # print 
          # print 'free:'
          # pprint.pprint(self.free)


        # ----------------------------------------------------------------------
        else:   # multi-node

            # walk through the list of nodes, and collect sufficiently many
            # cores and gpus until a CU can be served, store that away, and
            # start looking for the next slot; break once we run out of nodes.
            # NOTE: we switch to the next node as soon as we exhaust a node's
            #       cores or GPUs - whichever coes first will limit the max
            #       number of concurrent units anyway.

            core_map = list()
            gpu_map  = list()

            for node in self.nodes:

                node_uid  = node['uid']
                node_name = node['name']

                core_idx  = 0
                gpu_idx   = 0

                print 'reset idxs'

                # allocate chunks for as long as possible
                while True:

                    # do we still need cores?
                    while len(core_map) < cud['cpu_processes']:
                        # do we still have cores on this node:
                        if core_idx + cud['cpu_threads'] < self.cpn:
                            # use them
                            tmp = list()
                            for _ in range(cud['cpu_threads']):
                                tmp.append(core_idx)
                                core_idx += 1 
                            core_map.append(tmp)
                        else:
                            break

                    # do we still need gpu_map?
                    while len(gpu_map) < cud['gpu_processes']:
                        # do we still have gpu_map on this node:
                        if gpu_idx + 1 < self.gpn:
                            # use them
                            gpu_map.append([gpu_idx])
                            gpu_idx += 1 
                        else:
                            break

                    # is a chunk filled?
                    if  len(core_map) == cud['cpu_processes'] and \
                        len(gpu_map)  == cud['gpu_processes']:

                        slots = {'nodes'         : [[node_name, node_uid, core_map, gpu_map]],
                                 'cores_per_node': self.cpn,
                                 'gpus_per_node' : self.gpn,
                                 'lm_info'       : self._lrms_lm_info
                                 }
                        self.free.append(slots)
                        print 'got ', slots

                        core_map = list()
                        gpu_map  = list()

                    else:
                        print 'enough'
                        # no more slots on this node - go to next
                        break

        print 
        print 'free:'
        pprint.pprint(self.free)

        self._configured = True


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cud):
        '''
        This is the main method of this implementation, and is triggered when
        a unit needs to be mapped to a set of cores / gpus.
        '''

        if not self._configured:
            self._delayed_configure(cud)

        # ensure that all CUDs require the same amount of reources
        for k,v in self.chunk.iteritems():
            if cud[k] != v:
                raise ValueError('hetbre?  %d != %d' % (v, cud[k]))

        slots = self._find_slots(cud)

        return slots


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):
        '''
        This method is called when previously aquired resources are not needed
        anymore.  `slots` are the resource slots as previously returned by
        `_allocate_slots()`.
        '''

        with self.lock:
            for slot in slots:
                self.free.append(slots)


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

