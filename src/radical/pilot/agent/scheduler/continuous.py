
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentSchedulingComponent

import inspect
import threading as mt


# ------------------------------------------------------------------------------
#
# FIXME: make runtime switch depending on cprofile availability
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
class Continuous(AgentSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.nodes = None

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):

        if "CONTINUOUS" in os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "").split():
            self_thread = mt.current_thread()
            cprof.dump_stats("python-%s.profile" % self_thread.name)

        # make sure that parent finalizers are called
        super(Continuous, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # TODO: use real core/gpu numbers for non-exclusive reservations

        self.nodes = []
        for node, node_uid in self._lrms_node_list:
            self.nodes.append({
                'name' : node,
                'uid'  : node_uid,
                'cores': [rpc.FREE] * self._lrms_cores_per_node,
                'gpus' : [rpc.FREE] * self._lrms_gpus_per_node
            })


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        ret = "|"
        for node in self.nodes:
            for core in node['cores']:
                if core == rpc.FREE  : ret += '-'
                else                 : ret += '#'
            ret += ':'
            for gpu in node['gpus']  :
                if gpu == rpc.FREE   : ret += '-'
                else                 : ret += '#'
            ret += '|'

        return ret


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cud):

        # single_node is enforced for non-message passing tasks
        if cud['mpi']:
            slots = self._alloc_mpi(cud)
        else:
            slots = self._alloc_nompi(cud)


        if not slots:
            return {} # allocation failed

        self._change_slot_states(slots, rpc.BUSY)

        return slots


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):

        self._change_slot_states(slots, rpc.FREE)


    # --------------------------------------------------------------------------
    #
    def _alloc_node(self, node, requested_cores, requested_gpus, 
                    chunk=1, partial=False):
        '''
        Find up to the requested number of free cores and gpus in the node.
        This call will return two lists, for each matched set.  If the core does
        not have sufficient free resources to fullfill *both* requests, two
        empty lists are returned.  The call will *not* change the allocation
        status of the node, atomicity must be guaranteed by the caller.

        We don't care about continuity within a single node - cores `[1,5]` are
        assumed to be as close together as cores `[1,2]`.

        When `chunk` is set, only sets of exactly that size (or multiples
        thereof) are considered valid allocations.  The use case is OpenMP
        support, where each process is expected to create a certain number of
        threads, which thus must have slots on the same node available.

        NOTE: chunking is only applied to cores at this point.

        When `partial` is set to `True`, we also accept less cores and gpus then
        requested (but the call will never return more than requested).
        '''


        cores = list()
        gpus  = list()

        free_cores = node['cores'].count(rpc.FREE)
        free_gpus  = node['gpus' ].count(rpc.FREE)

        # first count the free cores/gpus, as that is way quicker than
        # actually finding the core IDs.
        if partial:
            # For partial requests the check simpliefies: we just check if we 
            # have either, some cores *or* gpus, to serve the request
            if  (requested_cores and not free_cores) and \
                (requested_gpus  and not free_gpus )     :
                # wa can't serve either request
                return [], []

        else:
            # non-partial requests (ie. full requests): check if we can serve
            # both, requested cores *and* gpus.
            if  requested_cores > free_cores or \
                requested_gpus  > free_gpus     :
                return [], []


        # we can serve the partial or full request - alloc the chunks we need
        # FIXME: chunk gpus, too?
        alloc_cores = min(requested_cores, free_cores) / chunk * chunk
        alloc_gpus  = min(requested_gpus , free_gpus )


        # now dig out the core and gpu IDs.
        while alloc_cores:
            for idx,state in enumerate(node['cores']):
                if state == rpc.FREE:
                    cores.append(idx)
                    alloc_cores -= 1

        while alloc_gpus:
            for idx,state in enumerate(node['gpus']):
                if state == rpc.FREE:
                    gpus.append(idx)
                    alloc_gpus -= 1

        return cores, gpus


    # --------------------------------------------------------------------------
    #
    def _alloc_nompi(self, cud):
        '''
        Find a suitable set of cores and gpus *within a single node*.
        '''

        requested_procs  = cud['cores']
        requested_gpus   = cud['gpus']
        threads_per_proc = cud['threads_per_proc']
        requested_cores  = requested_procs * threads_per_proc

        if  requested_cores > self._lrms_cores_per_node or \
            requested_gpus  > self._lrms_gpus_per_node:
            raise ValueError('Non-mpi unit does not fit onto single node')

        cores     = list()
        gpus      = list()
        node_name = None
        node_uid  = None

        # FIXME optimization: we should (re)start search not at the beginning,
        #       but where we left off the last time.
        for node in self.nodes:

            self._log.debug('try alloc on %s', node['uid'])

            cores, gpus = self._alloc_node(node, requested_cores, requested_gpus,
                                           chunk=threads_per_proc)

            if  len(cores) == requested_cores and \
                len(gpus)  == requested_gpus      :
                # we are done
                node_uid  = node['uid']
                node_name = node['name']
                break

        if  len(cores) < requested_cores or \
            len(gpus)  < requested_gpus     :
            # signal failure
            return None

        # we have to communicate to the launcher where exactly are processes to
        # be places, and what cores are reserved for application threads.
        core_map = list()
        gpu_map  = list()

        idx = 0
        for p in range(requested_procs):
            p_map = list()
            for t in range(threads_per_proc):
                p_map.append(cores[idx])
                idx += 1
            core_map.append(p_map)
        assert(idx == len(cores))

        # gpu procs are considered single threaded right now (FIXME)
        for g in gpus:
            gpu_map.append([g])

        slots = {'nodes'         : [[node_name, node_uid, core_map, gpu_map]],
                 'cores_per_node': self._lrms_cores_per_node, 
                 'gpus_per_node' : self._lrms_gpus_per_node,
                 'lm_info'       : self._lrms_lm_info
                 }

        return slots


    # --------------------------------------------------------------------------
    #
    #
    def _alloc_mpi(self, cud):
        ''''
        Find an available set of slots, potentially across node boundaries.  By
        default, we only allow for partial allocations on the first and last
        node - but all intermediate nodes MUST be completely used.  This way we
        allocate a *continuous* set of slots.

        If the scheduler is configured with `scattered=True`, then that
        constraint is relaxed, and any set of slots (continuous across nodes or
        not) is accepted as valid allocation.

        No matter the mode, we always make sure that we allocate in chunks of
        'threads_per_proc', as otherwise the application would not be able to
        spawn the requested number of threads on its slots.
        '''

        requested_procs  = cud['cores']
        requested_gpus   = cud['gpus']
        threads_per_proc = cud['threads_per_proc']
        requested_cores  = requested_procs * threads_per_proc
        scattered        = self._cfg.get('scattered', False)

        # First and last nodes can be a partial allocation - all other nodes can
        # only be partial when `scattered` is set.
        #
        # Iterate over all nodes until we find something. Check if it fits the
        # allocation mode and sequence.  If not, start over.  If it does, add
        # and continue to next node.
        #
        # FIXME: we should use a persistent node index to start searching where
        #        we last left off, instead of always starting from the beginning
        #        of the node list (optimization).
        slots = {'nodes'         : list(),
                 'cores_per_node': self._lrms_cores_per_node, 
                 'gpus_per_node' : self._lrms_gpus_per_node,
                 'lm_info'       : self._lrms_lm_info
                 }
        is_first      = True
        is_last       = False
        alloced_cores = 0
        alloced_gpus  = 0
        for node in nodes:
            node_uid  = node['uid ']
            node_name = node['name']

            if  requested_cores - alloced_cores <= cores_per_node and \
                requested_gpus  - alloced_gpus  <= gpus_per_node      :
                last = True

            if first or scattered: partial = True
            else                 : partial = False

            find_cores = requested_cores - alloced_cores
            find_gpus  = requested_gpus  - alloced_gpus 

            # ==================================================================
            # FIXME: should we do partial and chunkning check here or in
            #        _alloc_node?

            if not partial:
                # we want a full node, or at least the full remaining request
                find_cores = min(requested_cores, cores_per_node)
                find_gpus  = min(requested_gpus,  gpus_per_node )

            cores, gpus = self._alloc_node(node, requested_cores, requested_gpus,
                                           chunk=threads_per_proc,
                                           partial=partial)
            if cores or gpus:
                # we found something - this is good enough for now.  Keep what
                # we have, keep the idx to continue searching, and try to
                # allocate more (continuous or scattered) nodes
                if cores: alloced_cores.append([node['name'], cores])
                if gpus : alloced_gpus.append ([node['name'], gpus ])
                break

        # if we did not find anything, there is not much we can do at this point
        if not cores and not gpus:
            # signal failure
            return None

        
        # we will either keep adding cores and gpus from the
        # follow-up nodes continuously until we have the requested set of
        # resources complete, or the continuous block breaks, and we'll have to
        # start over from the first block.  Either way, we do



        # Glue all slot lists together
        # FIXME: optimization: maintain only one representation
        thing_str = ''
        for node in self.nodes:
            thing_str += node[thing_name]

      # self._log.debug("thing_str: %s", thing_str)

        # Find the start of the first available region
        first_index = thing_str.find(thing_needle)
        if first_index < 0:
            self._log.debug('no free %s found', thing_name)
            return [], []

        # Determine first node, and the first thing within that node
        first_node  = first_index / things_per_node
        first_thing = first_index % things_per_node

        # Note: We subtract one here, because counting starts at zero;
        #       Imagine a zero offset and a count of 1, the only core used
        #       would be core 0.
        #       TODO: Verify this claim :-)
        last_index = first_index + thing_num - 1
        last_node  = last_index / things_per_node
        last_thing = last_index % things_per_node

        self._log.debug("first index / node / %s: %s / %s / %s", 
                         first_index, first_node, thing_name, first_thing)
        self._log.debug("last  index / node / %s: %s / %s / %s", 
                         last_index, last_node, thing_name, last_thing)

        # Collect all slots here
        task_slots = list()

        if last_node == first_node:
            node_uid = self.nodes[first_node]['uid']
            for thing in range(first_thing, last_thing + 1):
                task_slots.append('%s:%d' % (node_uid, thing))
                self.nodes[thing_name][thing] = rpc.BUSY

        elif last_node != first_node:

            # add things from first node
            node_uid = self.nodes[first_node]['uid']
            for thing in range(first_thing, things_per_node):
                task_slots.append('%s:%d' % (node_uid, thing))
                self.nodes[thing_name][thing] = rpc.BUSY

            # add things from "middle" nodes
            for node_index in range(first_node+1, last_node-1):
                node_uid = self.nodes[node_index]['uid']
                for thing in range(0, things_per_node):
                    task_slots.append('%s:%d' % (node_uid, thing))
                    self.nodes[thing_name][thing] = rpc.BUSY

            # add things from last node
            node_uid = self.nodes[last_node]['uid']
            for thing in range(0, last_thing+1):
                task_slots.append('%s:%d' % (node_uid, thing))
                self.nodes[thing_name][thing] = rpc.BUSY

        return task_slots, [first_index]


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots (rpc.FREE or rpc.BUSY)
    #
    def _change_slot_states(self, slots, new_state):

        # FIXME: we don't know if we should change state for cores or gpus...

        import pprint
        self._log.debug('slots: %s', pprint.pformat(slots))


        for node_name, node_uid, cores, gpus in slots['nodes']:

            # Find the entry in the the slots list
            node = (n for n in self.nodes if n['uid'] == node_uid).next()
            assert(node)

            for cslot in cores:
                for core in cslot:
                    node['cores'][core] = new_state

            for gslot in gpus:
                for gpu in gslot:
                    node['gpus'][gpu] = new_state


# ------------------------------------------------------------------------------

