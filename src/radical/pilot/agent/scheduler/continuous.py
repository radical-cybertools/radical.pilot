
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from ...   import constants as rpc
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
    # FIXME: this should not be overloaded here, but in the base class
    #
    def finalize_child(self):

        cprof_env = os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "")
        if "CONTINUOUS" in cprof_env.split():
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

        self._scattered = self._cfg.get('scattered', False)


    # --------------------------------------------------------------------------
    #
    def slot_status(self):
        """Returns a multi-line string corresponding to slot status.
        """

        ret = "|"
        for node in self.nodes:
            for core in node['cores']:
                if core == rpc.FREE: ret += '-'
                else               : ret += '#'
            ret += ':'
            for gpu in node['gpus']:
                if gpu == rpc.FREE : ret += '-'
                else               : ret += '#'
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
            return {}  # allocation failed

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
            self._log.debug('%s -- %s -- %s -- %s',
                            idx, len(cores), cores, n_procs)
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

        # Things are complicated by chunking: we only accept chunks of
        # 'threads_per_proc', as otherwise threads would need to be distributed
        # over nodes, which is not possible for multi-system-image clusters this
        # scheduler assumes.  Thus scheduling will *always* fail if:
        #
        #   - requested_cores >= 3 * cores_per_node
        #   - cores_per_node % threads_per_proc != 0
        #   - scattered == False
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

        is_first      = True
        is_last       = False
        alloced_cores = 0
        alloced_gpus  = 0
        slots         = {'nodes'         : list(),
                         'cores_per_node': cores_per_node,
                         'gpus_per_node' : gpus_per_node,
                         'lm_info'       : self._lrms_lm_info
                         }

        for node in self.nodes:

            node_uid  = node['uid']
            node_name = node['name']

            if  requested_cores - alloced_cores <= cores_per_node and \
                requested_gpus  - alloced_gpus  <= gpus_per_node      :
                is_last = True

            if is_first or is_last or self._scattered or is_last: partial = True
            else                                                : partial = False

            find_cores  = min(requested_cores - alloced_cores, cores_per_node)
            find_gpus   = min(requested_gpus  - alloced_gpus,  gpus_per_node )
            cores, gpus = self._alloc_node(node, find_cores, find_gpus,
                                           chunk=threads_per_proc,
                                           partial=partial)

            if not cores and not gpus:
                # this was not a match. If we are not scattered, we have to
                # restart the search - otherwise we just ignore this node
                if not self._scattered:
                    is_first       = True
                    is_last        = False
                    alloced_cores  = 0
                    alloced_gpus   = 0
                    slots['nodes'] = list()

                # try next node
                break

            # we found something - add to the existing allocation, switch gears
            # (not first anymore), and try to find more if needed
            self._log.debug('found %s cores, %s gpus', cores, gpus)
            core_map, gpu_map = self._get_node_maps(cores, gpus, threads_per_proc)
            slots['nodes'].append([node_name, node_uid, core_map, gpu_map])

            alloced_cores += len(cores)
            alloced_gpus  += len(gpus)
            is_first       = False

            if  alloced_cores == requested_cores and \
                alloced_gpus  == requested_gpus      :
                # we are done
                break



        # if we did not find anything, there is not much we can do at this point
        if not cores and not gpus:
            # signal failure
            return None

        assert (alloced_cores == requested_cores)
        assert (alloced_gpus  == requested_gpus )

        return slots


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots (rpc.FREE or rpc.BUSY)
    #
    def _change_slot_states(self, slots, new_state):

        # FIXME: we don't know if we should change state for cores or gpus...
      # self._log.debug('slots: %s', pprint.pformat(slots))

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

