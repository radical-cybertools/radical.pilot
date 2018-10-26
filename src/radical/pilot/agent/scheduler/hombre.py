
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

        if single_node:
            if cores_needed > self.cpn or \
               gpus_needed  > self.gpn:
                    raise ValueError('unit does not fit on node')


        # ---------------------------------------------------------------------
        # create as many equal sized chunks from the available nodes as
        # possible, and put them into the `free` list.  The actual scheduling
        # algorithm will blindly pick chunks from that list whenever a new CUD
        # arrives.
        cblock   = cud['cpu_threads']
        ncblocks = cud['cpu_processes']
        cblocks  = list()
        cidx     = 0

        while cidx + cblock <= self.cpn:
            cblocks.append(range(cidx,cidx + cblock))
            cidx += cblock

        gblock  = 1
        ngblocks = cud['gpu_processes']
        gblocks = list()
        gidx    = 0
        while gidx + gblock < self.gpn:
            gblocks.append(range(gidx,gidx + gblock))
            gidx += gblock

        self._log.debug('==== core blocks %s', cblocks)
        self._log.debug('==== gpu  blocks %s', gblocks)

        for node in self.nodes:
            node['cblocks'] = copy.deepcopy(cblocks)
            node['gblocks'] = copy.deepcopy(gblocks)


        # ----------------------------------------------------------------------
        def next_slot(slot=None):
            if slot:
                del(slot['ncblocks'])
                del(slot['ngblocks'])
                self.free.append(slot)
            return {'nodes'         : list(),
                    'cores_per_node': self.cpn,
                    'gpus_per_node' : self.gpn,
                    'lm_info'       : self._lrms_lm_info, 
                    'ncblocks'      : 0, 
                    'ngblocks'      : 0}
        # ---------------------------------------------------------------------
        nidx   = 0
        nnodes = len(self.nodes)
        slot   = next_slot()
        while nidx < nnodes:

            if  slot['ncblocks'] == ncblocks and \
                slot['ngblocks'] == ngblocks :
                slot = next_slot(slot)

            node  = self.nodes[nidx]
            nuid  = node['uid']
            nname = node['name']
            nok   = True

            while slot['ncblocks'] < ncblocks:
                if node['cblocks']:
                    slot['nodes'].append([nname, nuid, [node['cblocks'].pop(0)], []])
                    slot['ncblocks'] += 1
                else:
                    nok = False
                    break

            while slot['ngblocks'] < ngblocks:
                if node['gblocks']:
                    slot['nodes'].append([nname, nuid, [], [node['gblocks'].pop(0)]])
                    slot['ngblocks'] += 1
                else:
                    nok = False
                    break

            if not nok:
                nidx += 1

        if  slot['ncblocks'] == ncblocks and \
            slot['ngblocks'] == ngblocks :
            self.free.append(slot)

        if not self.free:
            raise RuntimeError('configuration cannot be used for this workload')

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

