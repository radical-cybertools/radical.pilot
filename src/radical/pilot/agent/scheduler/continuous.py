
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"


import logging
import pprint

from ...   import constants as rpc
from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
# This is a continuous scheduler with awareness of a node's file-storage and
# unit tagging capabilities.
#
# Local Storage:
#
# The storage availability will be obtained from the lrms_node_list and assigned
# to the node list of the class. The lfs requirement will be obtained from the
# cud in the alloc_nompi and alloc_mpi methods. Using the availability and
# requirement, the _find_resources method will return the core and gpu ids.
#
# Expected DS of the nodelist
# self.nodes = [{
#                   'name'    : 'node_1',
#                   'uid'     : xxxx,
#                   'cores'   : [],
#                   'gpus'    : [],
#                   'lfs'     : 128
#               },
#               {
#                   'name'    : 'node_2',
#                   'uid'     : yyyy,
#                   'cores'   : [],
#                   'gpus'    : [],
#                   'lfs'     : 256
#                },
#              ]
#
# lfs storage is specified in MByte.  The scheduler assumes that storage is
# freed when the unit finishes.
#
#
# Unit Tagging:
#
# The scheduler attempts to schedule units with the same tag onto the same node,
# so that the unit can reuse the previous unit's data.  This assumes that
# storage is *not* freed when the units finishes.
#
#
# FIXME: the alert reader will realize a discrepancy in the above set of
#        assumptions.
#
# ------------------------------------------------------------------------------
#
class Continuous(AgentSchedulingComponent):
    '''
    The Continuous scheduler attempts to place threads and processes of
    a compute units onto consecutive cores, gpus and nodes in the cluster.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentSchedulingComponent.__init__(self, cfg, session)

        self._tag_history   = dict()
        self._scattered     = None
        self._oversubscribe = None


    # --------------------------------------------------------------------------
    #
    # FIXME: this should not be overloaded here, but in the base class
    #
    def finalize_child(self):

        # make sure that parent finalizers are called
        super(Continuous, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # * oversubscribe:
        #   Cray's aprun for example does not allow us to oversubscribe CPU
        #   cores on a node, so we can't, say, run n CPU processes on an n-core
        #   node, and than add one additional process for a GPU application.
        #
        #   If oversubscribe` is set to False (which is the default for now),
        #   we'll prevent that behavior by allocating one additional CPU core
        #   for each set of requested GPU processes.
        #
        #   FIXME: I think our scheme finds the wrong core IDs for GPU process
        #          startup - i.e. not the reserved ones.
        self._oversubscribe = self._cfg.get('oversubscribe', True)

        # * scattered:
        #   This is the continuous scheduler, because it attempts to allocate
        #   a *continuous* set of cores/nodes for a unit.  It does, however,
        #   also allow to scatter the allocation over discontinuous nodes if
        #   this option is set.  This implementation is not optimized for the
        #   scattered mode!  The default is 'False'.
        #
        self._scattered = self._cfg.get('scattered', False)

        # NOTE:  for non-oversubscribing mode, we reserve a number of cores
        #        for the GPU processes - even if those GPUs are not used by
        #        a specific workload.  In this case we rewrite the node list and
        #        substract the respective number of available cores per node.
        if not self._oversubscribe:

            if self._lrms_cores_per_node <= self._lrms_gpus_per_node:
                raise RuntimeError('oversubscription mode requires more cores')

            self._lrms_cores_per_node -= self._lrms_gpus_per_node

        # since we just changed this fundamental setting, we need to
        # recreate the nodelist.
        self.nodes = []
        for node, node_uid in self._lrms_node_list:

            node_entry = {'name'   : node,
                          'uid'    : node_uid,
                          'cores'  : [rpc.FREE] * self._lrms_cores_per_node,
                          'gpus'   : [rpc.FREE] * self._lrms_gpus_per_node,
                          'lfs'    :              self._lrms_lfs_per_node,
                          'mem'    :              self._lrms_mem_per_node}

            # summit
            if self._lrms_cores_per_node > 40:

                # Summit cannot address the last core of the second socket at
                # the moment, so we mark it as `DOWN` and the scheduler skips
                # it.  We need to check the SMT setting to make sure the right
                # logical cores are marked.  The error we see on those cores is:
                # "ERF error: 1+ cpus are not available"
                #
                # This is related to the known issue listed on
                # https://www.olcf.ornl.gov/for-users/system-user-guides \
                #                          /summit/summit-user-guide/
                #
                # "jsrun explicit resource file (ERF) allocates incorrect
                # resources"
                #
                smt = self._lrms_info.get('smt', 1)

                # only socket `1` is affected at the moment
              # for s in [0, 1]:
                for s in [1]:
                    for i in range(smt):
                        idx = s * 21 * smt + i
                        node_entry['cores'][idx] = rpc.DOWN

            self.nodes.append(node_entry)


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, unit):
        """
        attempt to allocate cores/gpus for a specific unit.
        """

        uid = unit['uid']

        # needs to be locked as we try to acquire slots here, but slots are
        # freed in a different thread.  But we keep the lock duration short...
        with self._slot_lock:

            self._prof.prof('schedule_try', uid=uid)
            unit['slots'] = self._allocate_slot(unit)

        # the lock is freed here
        if not unit['slots']:

            # signal the unit remains unhandled (False signals that failure)
            self._prof.prof('schedule_fail', uid=uid)
            return False


        # allocation worked!  If the unit was tagged, store the node IDs for
        # this tag, so that later units can reuse that information
        tag = unit['description'].get('tag')
        if tag:
            nodes = unit['slots']['nodes']
            self._tag_history[tag] = [node['uid'] for node in nodes]

        # translate gpu maps into `CUDA_VISIBLE_DEVICES` env
        self._handle_cuda(unit)

        # got an allocation, we can go off and launch the process
        self._prof.prof('schedule_ok', uid=uid)

        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("after  allocate   %s: %s", uid,
                            self.slot_status())
            self._log.debug("%s [%s/%s] : %s", uid,
                            unit['description']['cpu_processes'],
                            unit['description']['gpu_processes'],
                            pprint.pformat(unit['slots']))

        # True signals success
        return True


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, unit):
        '''
        This is the main method of this implementation, and is triggered when
        a unit needs to be mapped to a set of cores / gpus.  We make
        a distinction between MPI and non-MPI units (non-MPI processes MUST be
        on the same node).
        '''

        cud = unit['description']

        # single_node allocation is enforced for non-message passing tasks
        if cud['cpu_process_type'] in [rpc.MPI] or \
           cud['gpu_process_type'] in [rpc.MPI]:
            slots = self._alloc_mpi(unit)
        else:
            slots = self._alloc_nompi(unit)

        if slots:
            # the unit was placed, we need to reflect the allocation in the
            # nodelist state (BUSY)
            self._change_slot_states(slots, rpc.BUSY)

        return slots


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):
        '''
        This method is called when previously aquired resources are not needed
        anymore.  `slots` are the resource slots as previously returned by
        `_allocate_slots()`.
        '''

        # reflect the request in the nodelist state (set to `FREE`)
        self._change_slot_states(slots, rpc.FREE)


    # --------------------------------------------------------------------------
    #
    def _find_resources(self, node, requested_cores, requested_gpus,
                        requested_lfs, requested_mem, core_chunk=0,
                        partial=False, lfs_chunk=0, gpu_chunk=0, mem_chunk=0):
        '''
        Find up to the requested number of free cores and gpus in the node.
        This call will return two lists, for each matched set.  If the core
        does not have sufficient free resources to fulfill *both* requests, two
        empty lists are returned.  The call will *not* change the allocation
        status of the node, atomicity must be guaranteed by the caller.

        We don't care about continuity within a single node - cores `[1,5]` are
        assumed to be as close together as cores `[1,2]`.

        When `chunk` is set, only sets of exactly that size (or multiples
        thereof) are considered valid allocations.  The use case is OpenMP
        support, where each process is expected to create a certain number of
        threads, which thus must have slots on the same node available.

        NOTE: chunking is only applied to cores at this point.

        When `partial` is set to `True`, this method is allowed to return
        a *partial* match, so to find less cores, gpus, and local_fs then
        requested (but the call will never return more than requested).

        FIXME: SMT handling: we should assume that hardware threads of the same
               physical core cannot host different executables, so HW threads
               can only account for thread placement, not process placement.
               This might best be realized by internally handling SMT as minimal
               thread count and using physical core IDs for process placement.
        '''

        # list of core and gpu ids available in this node.
        cores = list()
        gpus  = list()
        lfs   = 0
        mem   = 0

        # first count the number of free cores, gpus, and local file storage.
        # This is way quicker than actually finding the core IDs.
        free_cores = node['cores'].count(rpc.FREE)
        free_gpus  = node['gpus'].count(rpc.FREE)
        free_lfs   = node['lfs']['size']
        free_mem   = node['mem']

        alloc_cores = 0
        alloc_gpus  = 0
        alloc_lfs   = 0
        alloc_mem   = 0

        # we need at least one core per gpu process
        requested_cores = max(requested_gpus, requested_cores)

        if partial:
            # For partial requests the check simplifies: we just check if we
            # have either, some cores *or* gpus, to serve the
            # request
            if (requested_cores and not free_cores) and \
               (requested_gpus  and not free_gpus)  :
                return [], [], None, None

        else:
            # For non-partial requests (ie. full requests): its a no-match if
            # any resource request cannot be served.
            if  requested_cores > free_cores or \
                requested_gpus  > free_gpus  or \
                requested_lfs   > free_lfs   or \
                requested_mem   > free_mem:
                return [], [], None, None

        # We can serve the partial or full request - alloc the chunks we need
        # FIXME: chunk gpus, too?
        # We need to land enough procs on a node such that the cores,
        # lfs and gpus requested per proc is available on the same node

        num_procs = dict()

        if requested_cores:
            alloc_cores = min(requested_cores, free_cores)
            num_procs['cores'] = alloc_cores / core_chunk

        if requested_gpus:
            alloc_gpus = min(requested_gpus, free_gpus)
            num_procs['gpus'] = alloc_gpus / gpu_chunk

        if requested_lfs:
            alloc_lfs = min(requested_lfs, free_lfs)
            num_procs['lfs'] = alloc_lfs / lfs_chunk

        if requested_mem:
            alloc_mem = min(requested_mem, free_mem)
            num_procs['mem'] = alloc_mem / mem_chunk


        # Find normalized cores, gpus and lfs
        if requested_cores: alloc_cores = num_procs['cores'] * core_chunk
        if requested_gpus : alloc_gpus  = num_procs['gpus']  * gpu_chunk
        if requested_lfs  : lfs         = num_procs['lfs']   * lfs_chunk
        if requested_mem  : mem         = num_procs['mem']   * mem_chunk

        self._log.debug('alc : %s %s %s %s', alloc_cores, alloc_gpus, lfs, mem)

        if requested_gpus:
            for idx, state in enumerate(node['gpus']):
                if state == rpc.FREE:        # use if free
                    gpus.append(idx)
                if alloc_gpus == len(gpus):  # break if enough
                    break

        # now dig out the core and gpu IDs.
        if requested_cores:
            for idx, state in enumerate(node['cores']):
                if state == rpc.FREE:           # use if free
                    cores.append(idx)
                if alloc_cores == len(cores):   # break if enough
                    break

        # if we found a gpu, we need at least one core in order to place
        # the process which uses that GPU.
        if gpus and not cores:
            gpus = list()

        # make sure we have full chunks
        if cores:
            n_core_chunks = len(cores) / core_chunk
            n_cores = n_core_chunks * core_chunk
            cores = cores[:n_cores]

        if gpus:
            n_gpu_chunks = len(gpus) / gpu_chunk
            n_gpus = n_gpu_chunks * gpu_chunk
            gpus = gpus[:n_gpus]

        return cores, gpus, lfs, mem


    # --------------------------------------------------------------------------
    #
    def _get_node_maps(self, cores, gpus, threads_per_proc):
        """
        For a given set of cores and gpus, chunk them into sub-sets so that
        each sub-set can host one application process and all threads of that
        process.  Note that we currently consider all GPU applications to be
        single-threaded.
        For more details, see top level comment of `base.py`.
        """
        self._log.debug('=== 5 found %s cores, %s gpus, %d tpp',
                            cores, gpus, threads_per_proc)
        self._log.debug('=== 5 found %s cores, %s gpus, %d tpp',
                            len(cores), len(gpus), threads_per_proc)

        core_map = list()
        gpu_map  = list()

        if cores:
            # make sure the core sets can host the requested number of threads
            assert(not len(cores) % threads_per_proc)
            n_procs =  len(cores) / threads_per_proc

            idx = 0
            for _ in range(n_procs):
                p_map = list()
                for _ in range(threads_per_proc):
                    p_map.append(cores[idx])
                    idx += 1
                core_map.append(p_map)

            if idx != len(cores):
                self._log.debug('%s -- %s -- %s -- %s',
                                idx, len(cores), cores, n_procs)
            assert(idx == len(cores))

        if gpus:
            # gpu procs are considered single threaded right now (FIXME)
            for g in gpus:
                gpu_map.append([g])

        return core_map, gpu_map


    # --------------------------------------------------------------------------
    #
    def _alloc_nompi(self, unit):
        """
        Find a suitable set of cores and gpus *within a single node*.

        Input:
        cud: Compute Unit description. Needs to specify at least one CPU
        process and one thread per CPU process, or one GPU process.
        """

        uid = unit['uid']
        cud = unit['description']

        # dig out the allocation request details
        lfs_chunk   = cud['lfs_per_process']
        core_chunk  = cud['cpu_threads']
        gpu_chunk   = cud['gpu_threads']
        mem_chunk   = cud['mem_per_process']
        total_cores = cud['cpu_processes'] * core_chunk
        total_gpus  = cud['gpu_processes']
        total_lfs   = cud['cpu_processes'] * lfs_chunk
        total_mem   = cud['cpu_processes'] * mem_chunk

        # make sure that the requested allocation fits on a single node
        if total_cores > self._lrms_cores_per_node or \
           total_gpus  > self._lrms_gpus_per_node  or \
           total_lfs   > self._lrms_lfs_per_node['size'] or \
           total_mem   > self._lrms_mem_per_node:

            txt  = 'Non-mpi unit %s does not fit onto node \n' % uid
            txt += 'cpus: %s >? %s\n' % (total_cores, self._lrms_cores_per_node)
            txt += 'gpus: %s >? %s\n' % (total_gpus,  self._lrms_gpus_per_node)
            txt += 'lfs : %s >? %s\n' % (total_lfs,   self._lrms_lfs_per_node)
            txt += 'mem : %s >? %s'   % (total_mem,   self._lrms_mem_per_node)
            raise ValueError(txt)

        if not core_chunk: core_chunk = 1
        if not gpu_chunk:  gpu_chunk  = 1

        # ok, we can go ahead and try to find a matching node
        node_name = None
        node_uid  = None
        cores     = list()
        gpus      = list()
        lfs       = None
        mem       = None
        tag       = cud.get('tag')

        for node in self.nodes:  # FIXME optimization: iteration start

            # Check if a unit is tagged to use this node.  This means we check
            #   - if a tag exists
            #   - if the tag has been used before
            #   - if the previous use included this node
            # Only continue to consider this node if the tag exists and was used
            # on this node - else continune to the next node.
            if tag:
                if tag in self._tag_history:
                    if node['uid'] not in self._tag_history[tag]:
                        continue

            # attempt to find the required number of cores and gpus on this
            # node - do not allow partial matches.
            cores, gpus, lfs, mem = self._find_resources(
                                                    node=node,
                                                    requested_cores=total_cores,
                                                    requested_gpus=total_gpus,
                                                    requested_lfs=total_lfs,
                                                    requested_mem=total_mem,
                                                    partial=False,
                                                    core_chunk=core_chunk,
                                                    gpu_chunk=gpu_chunk,
                                                    lfs_chunk=lfs_chunk,
                                                    mem_chunk=mem_chunk)
            if len(cores) >= total_cores and \
               len(gpus)  == total_gpus:

                # we found the needed resources - break out of search loop
                node_uid  = node['uid']
                node_name = node['name']
                break

        # If we did not find any node to host this request, return `None`
        if not node_name:
            return None

        # We have to communicate to the launcher where exactly processes are to
        # be placed, and what cores are reserved for application threads.  See
        # the top level comment of `base.py` for details on the data structure
        # used to specify process and thread to core mapping.
        core_map, gpu_map = self._get_node_maps(cores, gpus, core_chunk)

        # We need to specify the node lfs path that the unit needs to use.
        # We set it as an environment variable that gets loaded with cud
        # executable.
        # Assumption enforced: The LFS path is the same across all nodes.
        lfs_path = self._lrms_lfs_per_node['path']
        cud['environment']['NODE_LFS_PATH'] = lfs_path

        # all the information for placing the unit is acquired - return them
        slots = {'cores_per_node' : self._lrms_cores_per_node,
                 'gpus_per_node'  : self._lrms_gpus_per_node,
                 'lfs_per_node'   : self._lrms_lfs_per_node,
                 'mem_per_node'   : self._lrms_mem_per_node,
                 'lm_info'        : self._lrms_lm_info,
                 'nodes'          : [{'name'    : node_name,
                                      'uid'     : node_uid,
                                      'core_map': core_map,
                                      'gpu_map' : gpu_map,
                                      'lfs'     : {'size': lfs,
                                                   'path': lfs_path},
                                      'mem'     : mem
                                      }]}
        return slots


    # --------------------------------------------------------------------------
    #
    #
    def _alloc_mpi(self, unit):
        """
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
        """

        cud = unit['description']

        # dig out the allocation request details
        requested_procs  = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus   = cud['gpu_processes']
        gpu_chunk        = cud['gpu_threads']
        lfs_per_process  = cud['lfs_per_process']
        mem_per_process  = cud['mem_per_process']

        # make sure that processes are at least single-threaded
        if not threads_per_proc:
            threads_per_proc = 1

        if not gpu_chunk:  gpu_chunk  = 1

        # cores needed for all threads and processes
        requested_cores = requested_procs * threads_per_proc

        # We allocate the same lfs per process (agreement)
        requested_lfs = lfs_per_process * requested_procs

        # We allocate the same mem per process
        requested_mem = mem_per_process * requested_procs

        # First and last nodes can be a partial allocation - all other nodes
        # can only be partial when `scattered` is set.
        #
        # Iterate over all nodes until we find something. Check if it fits the
        # allocation mode and sequence.  If not, start over with the next node.
        # If it matches, add the slots found and continue to next node.
        #
        # FIXME: persistent node index

        cores_per_node = self._lrms_cores_per_node
        gpus_per_node  = self._lrms_gpus_per_node
        lfs_per_node   = self._lrms_lfs_per_node['size']
        mem_per_node   = self._lrms_mem_per_node

        # we always fail when too many threads are requested
        if threads_per_proc > cores_per_node:
            raise ValueError('too many threads requested')

        if lfs_per_process > lfs_per_node:
            raise ValueError('too much LFS requested')

        if mem_per_process > mem_per_node:
            raise ValueError('too much memory requested')

        # set conditions to find the first matching node
        is_first      = True
        is_last       = False
        alloced_cores = 0
        alloced_gpus  = 0
        alloced_lfs   = 0
        alloced_mem   = 0
        tag           = cud.get('tag')

        slots = {'nodes'         : list(),
                 'cores_per_node': self._lrms_cores_per_node,
                 'gpus_per_node' : self._lrms_gpus_per_node,
                 'lfs_per_node'  : self._lrms_lfs_per_node,
                 'mem_per_node'  : self._lrms_mem_per_node,
                 'lm_info'       : self._lrms_lm_info,
                }


        # start the search
        for node in self.nodes:

            node_uid  = node['uid']
            node_name = node['name']

            # Check if a unit is tagged to use this node.  This means we check
            #   - if a tag exists
            #   - if the tag has been used before
            #   - if the previous use included this node
            # Only continue to consider this node if the tag exists and was used
            # on this node - else continune to the next node.
            if tag:
                if tag in self._tag_history:
                    if node['uid'] not in self._tag_history[tag]:
                        continue

            # if only a small set of cores/gpus remains unallocated (ie. less
            # than node size), we are in fact looking for the last node.  Note
            # that this can also be the first node, for small units.
            if  requested_cores - alloced_cores <= cores_per_node and \
                requested_gpus  - alloced_gpus  <= gpus_per_node  and \
                requested_lfs   - alloced_lfs   <= lfs_per_node   and \
                requested_mem   - alloced_mem   <= mem_per_node:
                is_last = True

            # we allow partial nodes on the first and last node, and on any
            # node if a 'scattered' allocation is requested.
            if is_first or self._scattered or is_last:
                partial = True
            else:
                partial = False

            # now we know how many cores/gpus we still need at this point - but
            # we only search up to node-size on this node.  Duh!
            find_cores = min(requested_cores - alloced_cores, cores_per_node)
            find_gpus  = min(requested_gpus  - alloced_gpus,  gpus_per_node)
            find_lfs   = min(requested_lfs   - alloced_lfs,   lfs_per_node)
            find_mem   = min(requested_mem   - alloced_mem,   mem_per_node)

            # under the constraints so derived, check what we find on this node
            cores, gpus, lfs, mem = self._find_resources(node=node,
                                                    requested_cores=find_cores,
                                                    requested_gpus=find_gpus,
                                                    requested_lfs=find_lfs,
                                                    requested_mem=find_mem,
                                                    core_chunk=threads_per_proc,
                                                    partial=partial,
                                                    gpu_chunk=gpu_chunk,
                                                    lfs_chunk=lfs_per_process,
                                                    mem_chunk=mem_per_process)

            # Skip nodes that provide only lfs or mem but no cores/gpus
            if (lfs or mem) and (not cores and not gpus):
                continue

            if not cores and not gpus:

                # this was not a match. If we are in  'scattered' mode, we just
                # ignore this node.  Otherwise we have to restart the search
                # (continuity is broken)
                if not self._scattered:
                    slots['nodes'] = list()
                    alloced_cores  = 0
                    alloced_gpus   = 0
                    alloced_lfs    = 0
                    alloced_mem    = 0
                    is_first       = True
                    is_last        = False

                # try next node
                continue

            # we found something - add to the existing allocation, switch gears
            # (not first anymore), and try to find more if needed
            self._log.debug('found %s cores, %s gpus, %s lfs and %s mem',
                            cores, gpus, lfs, mem)
            core_map, gpu_map = self._get_node_maps(cores, gpus,
                                                    threads_per_proc)

            # We need to specify the node lfs path that the unit needs to use.
            # We set it as an environment variable that gets loaded with cud
            # executable.
            # Assumption enforced: The LFS path is the same across all nodes.
            lfs_path = self._lrms_lfs_per_node['path']
            if 'NODE_LFS_PATH' not in cud['environment']:
                cud['environment']['NODE_LFS_PATH'] = lfs_path

            slots['nodes'].append({'name'    : node_name,
                                   'uid'     : node_uid,
                                   'core_map': core_map,
                                   'gpu_map' : gpu_map,
                                   'lfs'     : {'size': lfs,
                                                'path': lfs_path},
                                   'mem'     : mem})

            alloced_cores += len(cores)
            alloced_gpus  += len(gpus)
            alloced_lfs   += lfs
            alloced_mem   += mem

            is_first       = False

            # or maybe don't continue the search if we have in fact enough!
            if  alloced_cores == requested_cores and \
                alloced_gpus  == requested_gpus  and \
                alloced_lfs   == requested_lfs   and \
                alloced_mem   == requested_mem:
                # we are done
                break

        # if we did not find enough, there is not much we can do at this point
        if  alloced_cores < requested_cores or \
            alloced_gpus  < requested_gpus  or \
            alloced_lfs   < requested_lfs   or \
            alloced_mem   < requested_mem:
            return None  # signal failure

        # this should be nicely filled out now - return
        return slots


# ------------------------------------------------------------------------------

