
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import pprint

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import AgentSchedulingComponent

import inspect
import threading as mt


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
    # FIXME: this should not be overloaded here, but in the base class
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
        #
        # * oversubscribe:
        #   Cray's aprun for example does not allow us to oversubscribe CPU
        #   cores on a node, so we can't, say, run n CPU processes on an n-core
        #   node, and than add one additional process for a GPU application.  If
        #   `oversubscribe` is set to False (which is the default for now),
        #   we'll prevent that behavior by allocating one additional CPU core
        #   for each requested GPU process.
        #
        # * scattered:
        #   This is the continuous scheduler, because it attempts to allocate
        #   a *continuous* set of cores/nodes for a unit.  It does, hoewver,
        #   also allow to scatter the allocation over discontinuous nodes if
        #   this option is set.  the default is 'False'.
        self._oversubscribe = self._cfg.get('oversubscribe', False)
        self._scattered     = self._cfg.get('scattered',     False)

        # NOTE: for non-oversubscribing mode, we reserve a number of cores
        #       for the GPU processes - even if those GPUs are not used by
        #       a specific workload.
        if not self._oversubscribe:
            self._lrms_cores_per_node -= self._lrms_gpus_per_node

            # since we just changed this fundamental setting, we need to
            # recreate the nodelist.
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
    def _allocate_slot(self, cud):
        '''
        This is the main method of this implementation, and is triggered when
        a unit needs to be mapped to a set of cores / gpus.  We make
        a distinction between MPI and non-MPI units (non-MPI processes MUST be
        on the same node).
        '''

        # single_node is enforced for non-message passing tasks
        if  cud['cpu_process_type'] == 'MPI' or \
            cud['gpu_process_type'] == 'MPI' :
            slots = self._alloc_mpi(cud)
        else:
            slots = self._alloc_nompi(cud)

        if slots:
            # the unit was placed, we need to reflect the allocation in the
            # nodelist state
            self._change_slot_states(slots, rpc.BUSY)

        return slots


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):
        '''
        As the opposite to `_allocate_slot()`, this method will be invoked if
        a unit does no longer need the resources previously allocation above.
        '''

        # reflect the request in the nodelist state.
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
        for idx,state in enumerate(node['cores']):
            if alloc_cores == len(cores):
                break
            if state == rpc.FREE:
                cores.append(idx)

        for idx,state in enumerate(node['gpus']):
            if alloc_gpus == len(gpus):
                break
            if state == rpc.FREE:
                gpus.append(idx)

        return cores, gpus


    # --------------------------------------------------------------------------
    #
    def _get_node_maps(self, cores, gpus, threads_per_proc):

        core_map = list()
        gpu_map  = list()

        assert(not len(cores) % threads_per_proc)
        n_procs =  len(cores) / threads_per_proc

        idx = 0
        for p in range(n_procs):
            p_map = list()
            for t in range(threads_per_proc):
                p_map.append(cores[idx])
                idx += 1
            core_map.append(p_map)

        if idx != len(cores):
            self._log.debug('%s -- %s -- %s -- %s', idx, len(cores), cores, n_procs)
        assert(idx == len(cores))

        # gpu procs are considered single threaded right now (FIXME)
        for g in gpus:
            gpu_map.append([g])

        return core_map, gpu_map


    # --------------------------------------------------------------------------
    #
    def _alloc_nompi(self, cud):
        '''
        Find a suitable set of cores and gpus *within a single node*.
        '''

        # dig out the allocation request details
        requested_procs  = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus   = cud['gpu_processes']

        if not threads_per_proc:
            threads_per_proc = 1

        requested_cores = requested_procs * threads_per_proc

        # we want the allocation to fit on a single node - make sure this is
        # actually possible
        if  requested_cores > self._lrms_cores_per_node or \
            requested_gpus  > self._lrms_gpus_per_node     :
            raise ValueError('Non-mpi unit does not fit onto single node')

        # ok, we can go ahead and try to find a matching node.
        cores     = list()
        gpus      = list()
        node_name = None
        node_uid  = None
        for node in self.nodes:  # FIXME optimization: iteration start

            # attempt to allocate the required number of cores and gpus on this
            # node - do not allow partial matches.
            cores, gpus = self._alloc_node(node, requested_cores, requested_gpus,
                                           partial=False)

            if  len(cores) == requested_cores and \
                len(gpus)  == requested_gpus      :
                # we are done
                node_uid  = node['uid']
                node_name = node['name']
                break

        if not cores and not gpus:
            return None  # signal failure

        # we have to communicate to the launcher where exactly processes are to
        # be placed, and what cores are reserved for application threads.
        core_map, gpu_map = self._get_node_maps(cores, gpus, threads_per_proc)

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
        node - but all intermediate nodes MUST be completely used (this is the
        'CONTINUOUS' scheduler after all).

        If the scheduler is configured with `scattered=True`, then that
        constraint is relaxed, and any set of slots (be it continuous across
        nodes or not) is accepted as valid allocation.

        No matter the mode, we always make sure that we allocate in chunks of
        'threads_per_proc', as otherwise the application would not be able to
        spawn the requested number of threads on the respective node.
        '''

        requested_procs  = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus   = cud['gpu_processes']

        if not threads_per_proc:
            threads_per_proc = 1

        requested_cores = requested_procs * threads_per_proc   # define if the requested_cores refer to physical, logical

        # First and last nodes can be a partial allocation - all other nodes can
        # only be partial when `scattered` is set.
        #
        # Iterate over all nodes until we find something. Check if it fits the
        # allocation mode and sequence.  If not, start over with the next node.
        # If it matches, add the slots found and continue to next node.
        #
        # FIXME: persistent node index
        #
        # Things are complicated by chunking: we only accept chunks of
        # 'threads_per_proc', as otherwise threads would need to be distributed
        # over nodes, which is not possible for the multi-system-image clusters
        # this scheduler assumes.
        #
        #   - requested_cores > cores_per_node
        #   - cores_per_node  % threads_per_proc != 0
        #   - scattered is False
        #
        # but it can fail for less cores, too, if the partial first and last
        # allocation are not favorable.  We thus raise an exception for
        # requested_cores > cores_per_node on impossible full-node-chunking

        cores_per_node = self._lrms_cores_per_node
        gpus_per_node  = self._lrms_gpus_per_node
        
        if  requested_cores  > cores_per_node   and \
            cores_per_node   % threads_per_proc and \
            self._scattered is False:
            raise ValueError('cannot allocate under given constrains')

        # we always fail when too many threads are requested
        if threads_per_proc > cores_per_node:
            raise ValueError('too many threads requested')

        # set conditions to find the first matching node
        is_first      = True
        is_last       = False
        alloced_cores = 0
        alloced_gpus  = 0
        slots         = {'nodes'         : list(),
                         'cores_per_node': cores_per_node,
                         'gpus_per_node' : gpus_per_node,
                         'lm_info'       : self._lrms_lm_info
                         }

        # start the search
        for node in self.nodes: 
            ## GEORGE: How to improve this search? 
            ## IDEA: Define weights for cpus & gpus and create a function f(node) = a*cpus + b*gpus
            ## sort based on function f()

            node_uid  = node['uid']
            node_name = node['name']

            # if only a small set of cores/gpus remains unallocated (ie. less
            # than node size), we are in fact looking for the last node.  Note
            # that this can also be the first node, for small units.
            if  requested_cores - alloced_cores <= cores_per_node and \
                requested_gpus  - alloced_gpus  <= gpus_per_node      :
                is_last = True

            # we allow partial nodes on the first and last node, and on any
            # node if a 'scattered' allocation is requested.
            if is_first or self._scattered or is_last: partial = True
            else                                     : partial = False

            # now we know how many cores/gpus we still need at this point - but
            # we only search up to node-size on this node.  Duh!
            find_cores  = min(requested_cores - alloced_cores, cores_per_node)
            find_gpus   = min(requested_gpus  - alloced_gpus,  gpus_per_node )

            # under the constraints so derived, check what we find on this node
            cores, gpus = self._alloc_node(node, find_cores, find_gpus,   ##GEORGE:  Change _alloc_node to node_availability    
                                           chunk=threads_per_proc,   
                                           partial=partial)


            # and check the result.
            if not cores and not gpus:

                # this was not a match. If we are in  'scattered' mode, we just
                # ignore this node.  Otherwise we have to restart the search.
                if not self._scattered:
                    is_first       = True
                    is_last        = False
                    alloced_cores  = 0
                    alloced_gpus   = 0
                    slots['nodes'] = list()

                # try next node
                continue

            # we found something - add to the existing allocation, switch gears
            # (not first anymore), and try to find more if needed
            self._log.debug('found %s cores, %s gpus', cores, gpus)
            core_map, gpu_map = self._get_node_maps(cores, gpus, threads_per_proc)
            slots['nodes'].append([node_name, node_uid, core_map, gpu_map])
            
            alloced_cores += len(cores)
            alloced_gpus  += len(gpus)
            is_first       = False

            # or maybe don't continue the search if we have in fact enough!
            if  alloced_cores == requested_cores and \
                alloced_gpus  == requested_gpus      :
                # we are done
                break

        # if we did not find enough, there is not much we can do at this point
        if  alloced_cores < requested_cores or \
            alloced_gpus  < requested_gpus     :
            return None # signal failure         ## GEORGE: Define what failure signal is that. Fail alloc? 
                                                 ## is the same signal for all failures?

        # this should be nicely filled out now - return
        return slots


# ------------------------------------------------------------------------------

