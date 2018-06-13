
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


from .continuous import Continuous


# ------------------------------------------------------------------------------
#
# This is a simle extension of the Continuous scheduler which makes RP behave
# like a FiFo: it will only really attempt to schedule units if all older units
# (units with lower unit ID) have been scheduled.  As such, it relies on unit
# IDs to be of the format `unit.%d'`.
#
class Tagging(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.slots = None
        self._tag_history = dict()

        Continuous.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _alloc_nompi(self, cud):
        """
        Find a suitable set of cores and gpus *within a single node*.

        Input:
        cud: Compute Unit description. Needs to specify at least one CPU
        process and one thread per CPU process, or one GPU process.
        """

        # dig out the allocation request details
        requested_procs = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus = cud['gpu_processes']
        requested_lfs = cud['lfs_per_process']
        lfs_chunk = cud['lfs_per_process'] if cud['lfs_per_process'] > 0 else 1
        tag = cud.get('tag')
        uid = cud['uid']

        # make sure that processes are at least single-threaded
        if not threads_per_proc:
            threads_per_proc = 1

        # cores needed for all threads and processes
        requested_cores = requested_procs * threads_per_proc

        # make sure that the requested allocation fits on a single node
        if  requested_cores > self._lrms_cores_per_node or \
            requested_gpus > self._lrms_gpus_per_node or \
            requested_lfs > self._lrms_lfs_per_node['size']:  
            raise ValueError('Non-mpi unit does not fit onto single node')

        # ok, we can go ahead and try to find a matching node
        cores = list()
        gpus = list()
        lfs = None
        node_name = None
        node_uid = None

        for node in self.nodes:  # FIXME optimization: iteration start

            
            if tag and (tag in self._tag_history.keys()):
                if node['uid'] not in self._tag_history[tag]:
                    continue

            # attempt to find the required number of cores and gpus on this
            # node - do not allow partial matches.
            cores, gpus, lfs = self._find_resources(node,
                                                    requested_cores,
                                                    requested_gpus,
                                                    requested_lfs,
                                                    partial=False,
                                                    lfs_chunk=lfs_chunk
                                                    )
            if len(cores) == requested_cores and \
                    len(gpus) == requested_gpus:
                # we found the needed resources - break out of search loop
                node_uid = node['uid']
                node_name = node['name']
                break

        # If we did not find any node to host this request, return `None`
        if not cores and not gpus and not lfs:
            return None

        # Store the selected node uid in the tag history
        self._tag_history[uid] = [node['uid']]

        # We have to communicate to the launcher where exactly processes are to
        # be placed, and what cores are reserved for application threads.  See
        # the top level comment of `base.py` for details on the data structure
        # used to specify process and thread to core mapping.
        core_map, gpu_map = self._get_node_maps(cores, gpus, threads_per_proc)

        # all the information for placing the unit is acquired - return them
        slots = {'nodes': [{'name': node_name,
                            'uid': node_uid,
                            'core_map': core_map,
                            'gpu_map': gpu_map,
                            'lfs': lfs}],
                 'cores_per_node': self._lrms_cores_per_node,
                 'gpus_per_node': self._lrms_gpus_per_node,
                 'lm_info': self._lrms_lm_info
                 }
        return slots

    # --------------------------------------------------------------------------


    def _alloc_mpi(self, cud):
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

        # dig out the allocation request details
        requested_procs = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus = cud['gpu_processes']
        requested_lfs_per_process = cud['lfs_per_process']
        tag = cud.get('tag')
        uid = cud['uid']

        # make sure that processes are at least single-threaded
        if not threads_per_proc:
            threads_per_proc = 1

        # cores needed for all threads and processes
        requested_cores = requested_procs * threads_per_proc

        # We allocate the same lfs per process (agreement)
        requested_lfs = requested_lfs_per_process * requested_procs

        # First and last nodes can be a partial allocation - all other nodes
        # can only be partial when `scattered` is set.
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
        gpus_per_node = self._lrms_gpus_per_node
        lfs_per_node = self._lrms_lfs_per_node   # this is a dictionary
        # which have two key,s size (MB) and path

        if requested_cores > cores_per_node and \
                cores_per_node % threads_per_proc and \
                self._scattered is False:
            raise ValueError('cannot allocate under given constrains')

        # we always fail when too many threads are requested
        if threads_per_proc > cores_per_node:
            raise ValueError('too many threads requested')

        if requested_lfs_per_process > lfs_per_node['size']:
            raise ValueError('Not enough LFS for the MPI-process')

            # set conditions to find the first matching node
        is_first = True
        is_last = False
        alloced_cores = 0
        alloced_gpus = 0
        alloced_lfs = 0   # LFS is in MBs

        slots = {'nodes': list(),
                 'cores_per_node': cores_per_node,
                 'gpus_per_node': gpus_per_node,
                 'lfs_per_node': lfs_per_node,
                 'lm_info': self._lrms_lm_info,
                 }

        # start the search
        for node in self.nodes:

            node_uid = node['uid']
            node_name = node['name']

            if tag and (tag in self._tag_history.keys()):
                if node['uid'] not in self._tag_history[tag]:
                    continue

            # if only a small set of cores/gpus remains unallocated (ie. less
            # than node size), we are in fact looking for the last node.  Note
            # that this can also be the first node, for small units.
            if  requested_cores - alloced_cores <= cores_per_node and \
                requested_gpus - alloced_gpus <= gpus_per_node and \
                requested_lfs - alloced_lfs <= lfs_per_node['size']:
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
            find_gpus = min(requested_gpus - alloced_gpus,  gpus_per_node)
            find_lfs = min(requested_lfs - alloced_lfs, lfs_per_node['size'])

            # under the constraints so derived, check what we find on this node
            cores, gpus, lfs = self._find_resources(node=node,
                                                    requested_cores=find_cores,
                                                    requested_gpus=find_gpus,
                                                    requested_lfs=find_lfs,
                                                    core_chunk=threads_per_proc,
                                                    partial=partial,
                                                    lfs_chunk=requested_lfs_per_process)


            # and check the result.
            if not cores and not gpus and not lfs:

                # this was not a match. If we are in  'scattered' mode, we just
                # ignore this node.  Otherwise we have to restart the search.
                if not self._scattered:
                    is_first = True
                    is_last = False
                    alloced_cores = 0
                    alloced_gpus = 0
                    alloced_lfs = 0
                    slots['nodes'] = list()

                # try next node
                continue


            # Store the selected node uid in the tag history
            if uid not in self._tag_history.keys():
                self._tag_history[uid] = [node['uid']]
            else:
                self._tag_history[uid].append(node['uid'])

            # we found something - add to the existing allocation, switch gears
            # (not first anymore), and try to find more if needed
            self._log.debug('found %s cores, %s gpus and %s lfs', cores, gpus,
                            lfs)
            core_map, gpu_map = self._get_node_maps(cores, gpus,
                                                    threads_per_proc)

            slots['nodes'].append({'name': node_name,
                                   'uid': node_uid,
                                   'core_map': core_map,
                                   'gpu_map': gpu_map,
                                   'lfs': lfs})

            # Keys in a slot
            # 'nodes': [{'name': node_name,
            #                 'uid': node_uid,
            #                 'core_map': core_map,
            #                 'gpu_map': gpu_map,
            #                 'lfs': lfs}],
            #      'cores_per_node': self._lrms_cores_per_node,
            #      'gpus_per_node': self._lrms_gpus_per_node,
            #      'lm_info': self._lrms_lm_info

            alloced_cores += len(cores)
            alloced_gpus += len(gpus)
            alloced_lfs += lfs
            is_first = False

            # or maybe don't continue the search if we have in fact enough!
            if  alloced_cores == requested_cores and \
                alloced_gpus == requested_gpus and \
                alloced_lfs == requested_lfs:
                # we are done
                break

        # if we did not find enough, there is not much we can do at this point
        if  alloced_cores < requested_cores or \
            alloced_gpus < requested_gpus or \
            alloced_lfs < requested_lfs:
            return None  # signal failure

        # this should be nicely filled out now - return
        return slots