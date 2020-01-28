
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"


import os
import pprint
import inspect
import logging

import radical.utils as ru

from ...   import constants as rpc
from ...   import states    as rps
from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
# This is an extension of the continuous scheduler with awareness of the
# file-storage capabilities on a node. The continuous data aware scheduler will
# use two data fields: availability and requirement.

# General idea:
# The availability will be obtained from the rm_node_list and assigned to
# the node list of the class. The requirement will be obtained from the cud in
# the alloc_nompi and alloc_mpi methods. Using the availability and
# requirement, the _find_resources method will return the core and gpu ids.
#
# Expected DS of the nodelist
# self.nodes = [{
#                   'name'    : 'node_1',
#                   'uid'     : xxxx,
#                   'sockets' : [  {'cores': [], 'gpus': []},
#                                  {'cores': [], 'gpus': []}]
#                   'lfs'     : 128
#               },
#               {
#                   'name'    : 'node_2',
#                   'uid'     : yyyy,
#                   'sockets' : [  {'cores': [], 'gpus': []},
#                                  {'cores': [], 'gpus': []}]
#                   'lfs'     : 256
#                },
#               ]
# Q: How should the nodes be selected for MPI based units?
# lfs : in mb

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
            cprof_env = os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "")
            cprof_elems = cprof_env.split()
            if "CONTINUOUS" in cprof_elems:
                for name, m in inspect.getmembers(cls, inspect.ismethod):
                    setattr(cls, name, dec(m))
        return cls
    return dectheclass


# ------------------------------------------------------------------------------
#
@dec_all_methods(cprof_it)
class ContinuousSummit(AgentSchedulingComponent):
    '''
    The Continuous scheduler attempts to place threads and processes of
    a compute units onto consecutive cores, gpus and nodes in the cluster.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.nodes = None
        self._tag_history = dict()

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize()` will be called
    # before control is given to the component's main loop.
    #
    def initialize(self):

        # register unit input channels
        self.register_input(rps.AGENT_SCHEDULING_PENDING,
                            rpc.AGENT_SCHEDULING_QUEUE, self._schedule_units)

        # register unit output channels
        self.register_output(rps.AGENT_EXECUTING_PENDING,
                             rpc.AGENT_EXECUTING_QUEUE)

        # we need unschedule updates to learn about units for which to free the
        # allocated cores.  Those updates MUST be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.register_subscriber(rpc.AGENT_UNSCHEDULE_PUBSUB, self.unschedule_cb)

        # we don't want the unschedule above to compete with actual
        # scheduling attempts, so we move the re-scheduling of units from the
        # wait pool into a separate thread (ie. register a separate callback).
        # This is triggered by the unscheduled_cb.
        #
        # NOTE: we could use a local queue here.  Using a zmq bridge goes toward
        #       an distributed scheduler, and is also easier to implement right
        #       now, since `Component` provides the right mechanisms...
        self.register_publisher(rpc.AGENT_SCHEDULE_PUBSUB)
        self.register_subscriber(rpc.AGENT_SCHEDULE_PUBSUB, self.schedule_cb)

        # The scheduler needs the ResourceManager information which have been collected
        # during agent startup.  We dig them out of the config at this point.
        #
        # NOTE: this information is insufficient for the torus scheduler!
        self._pid                   = self._cfg['pid']
        self._rm_info             = self._cfg['rm_info']
        self._rm_lm_info          = self._cfg['rm_info']['lm_info']
        self._rm_node_list        = self._cfg['rm_info']['node_list']
        self._rm_sockets_per_node = self._cfg['rm_info']['sockets_per_node']
        self._rm_cores_per_socket = self._cfg['rm_info']['cores_per_socket']
        self._rm_gpus_per_socket  = self._cfg['rm_info']['gpus_per_socket']
        self._rm_lfs_per_node     = self._cfg['rm_info']['lfs_per_node']

        if not self._rm_node_list:
            raise RuntimeError("ResourceManager %s didn't _configure node_list."
                              % self._rm_info['name'])

        if self._rm_cores_per_socket is None:
            raise RuntimeError("ResourceManager %s didn't _configure cores_per_socket."
                              % self._rm_info['name'])

        if self._rm_sockets_per_node is None:
            raise RuntimeError("ResourceManager %s didn't _configure sockets_per_node."
                              % self._rm_info['name'])

        if self._rm_gpus_per_socket is None:
            raise RuntimeError("ResourceManager %s didn't _configure gpus_per_socket."
                              % self._rm_info['name'])

        # create and initialize the wait pool
        self._wait_pool = list()      # pool of waiting units
        self._wait_lock = ru.RLock()  # look on the above pool
        self._slot_lock = ru.RLock()  # lock slot allocation/deallocation

        # configure the scheduler instance
        self._configure()
        self._log.debug("slot status after  init      : %s",
                        self.slot_status())


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # * oversubscribe:
        #   Cray's aprun for example does not allow us to oversubscribe CPU
        #   cores on a node, so we can't, say, run n CPU processes on an n-core
        #   node, and than add one additional process for a GPU application.
        # If oversubscribe` is set to False (which is the default for now),
        #   we'll prevent that behavior by allocating one additional CPU core
        #   for each set of requested GPU processes.
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


        # * cross_socket_threads:
        # We now capture the knowledge of sockets on nodes and the relationship
        # of cores to sockets and gpus to sockets. We want to ensure that cpu
        # threads belonging to a single process do not cross sockets if the
        # system does not allow it. The default is True, based on discussions
        # in the Summit meeting, and will be set to False for Summit in the
        # resource config.
        self._cross_socket_threads = self._cfg.get('cross_socket_threads', True)

        # NOTE:  for non-oversubscribing mode, we reserve a number of cores
        #        for the GPU processes - even if those GPUs are not used by
        #        a specific workload.  In this case we rewrite the node list and
        #        substract the respective number of available cores per socket.
        if not self._oversubscribe:

            if self._rm_cores_per_socket <= self._rm_gpus_per_socket:
                raise RuntimeError('oversubscription mode requires more cores')

            self._rm_cores_per_socket -= self._rm_gpus_per_socket


        # since we just changed this fundamental setting, we need to
        # recreate the nodelist.
        self.nodes = []
        for node, node_uid in self._rm_node_list:

            node_entry = {'name'   : node,
                          'uid'    : node_uid,
                          'sockets': list(),
                          'lfs'    : self._rm_lfs_per_node}

            for socket in range(self._rm_sockets_per_node):
                node_entry['sockets'].append({
                    'cores': [rpc.FREE] * self._rm_cores_per_socket,
                    'gpus' : [rpc.FREE] * self._rm_gpus_per_socket
                })

            self.nodes.append(node_entry)


    # --------------------------------------------------------------------------
    #
    # Change the reserved state of slots (rpc.FREE or rpc.BUSY)
    #
    # NOTE: any scheduler implementation which uses a different nodelist
    #       structure MUST overload this method.
    #
    def _change_slot_states(self, slots, new_state):
        '''
        This function is used to update the state for a list of slots that
        have been allocated or deallocated.  For details on the data structure,
        see top of `base.py`.
        '''
        # This method needs to change if the DS changes.
        # Current slot DS:
        # slots = {'nodes': [{'name'    : node_name,
        #                     'uid'     : node_uid,
        #                     'core_map': core_map,
        #                     'gpu_map' : gpu_map,
        #                     'lfs'     : {'size': lfs,
        #                                  'path': self._rm_lfs_per_node['path']
        #                                 }
        #                    }],
        #          'cores_per_node' : cores_per_node,
        #          'gpus_per_node'  : gpus_per_node,
        #          'lfs_per_node'   : self._rm_lfs_per_node,
        #          'lm_info'        : self._rm_lm_info
        #          }
        #
        # self.nodes = [{
        #                   'name'    : 'node_1',
        #                   'uid'     : xxxx,
        #                   'sockets' : [   {'cores': [], 'gpus': []},
        #                                   {'cores': [], 'gpus': []}]
        #                   'lfs'     : 128}],


        # for node_name, node_uid, cores, gpus in slots['nodes']:
        for node in slots['nodes']:

            # Find the entry in the the slots list

            # TODO: [Optimization] Assuming 'uid' is the ID of the node, it
            #       seems a bit wasteful to have to look at all of the nodes
            #       available for use if at most one node can have that uid.
            #       Maybe it would be worthwhile to simply keep a list of nodes
            #       that we would read, and keep a dictionary that maps the uid
            #       of the node to the location on the list?

            for entry in self.nodes:
                if entry['uid'] == node['uid']:
                    break

            assert(entry), 'missing node %s' % node['uid']

            # STRONG ASSUMPTION!!!
            # We assume that the core and gpu ids are continuous on a socket
            # indexed with core_id=0 on socket_id=0 and gpu_id= on socket_id=0.
            # For example, with the following node config for Summit:
            # sockets_per_node=2, cores_per_socket=21, gpus_per_socket=3
            # Our assumption leads to the following id relations:

            # node['sockets'] = [
            #     {   'uid': 0,     # not actually present in socket list
            #         'core_ids': [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            #                      10,11,12,13,14,15,16,17,18,19,
            #                      20],
            #         'gpu_ids':  [0,1,2]
            #     }
            #     {   'uid': 1,    # not actually present in socket list
            #         'core_ids': [21,22,23,24,25,26,27,28,29,30,
            #                      31,32,33,34,35,36,37,38,39,40,
            #                      41],
            #         'gpu_ids':  [3,4,5]
            #     }
            # ]

            # iterate over resources in the slot, and update state
            for cslot in node['core_map']:
                for core in cslot:
                    socket  = core / self._rm_cores_per_socket
                    core_id = core % self._rm_cores_per_socket
                    entry['sockets'][socket]['cores'][core_id] = new_state

            for gslot in node['gpu_map']:
                for gpu in gslot:
                    socket = gpu / self._rm_gpus_per_socket
                    gpu_id = gpu % self._rm_gpus_per_socket
                    entry['sockets'][socket]['gpus'][gpu_id] = new_state

            if entry['lfs']['path']:
                if new_state == rpc.BUSY:
                    entry['lfs']['size'] -= node['lfs']['size']
                else:
                    entry['lfs']['size'] += node['lfs']['size']


    # --------------------------------------------------------------------------
    #
    # Overloaded from Base
    def slot_status(self):
        '''
        Returns a multi-line string corresponding to the status of the node list
        '''

        ret = "|"
        for node in self.nodes:
            for socket in node['sockets']:
                for core in socket['cores']:
                    if core == rpc.FREE:
                        ret += '-'
                    else:
                        ret += '#'
                ret += ':'
                for gpu in socket['gpus']:
                    if gpu == rpc.FREE:
                        ret += '-'
                    else:
                        ret += '#'
                ret += '|'

        return ret


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

            # signal the unit remains unhandled (Fales signals that failure)
            self._prof.prof('schedule_fail', uid=uid)
            return False


        node_uids = [node['uid'] for node in unit['slots']['nodes']]
        self._tag_history[uid] = node_uids

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

        uid = unit['uid']
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
                        requested_lfs, core_chunk=1, partial=False,
                        lfs_chunk=1, gpu_chunk=1):
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
        '''

        # list of core and gpu ids available in this node.
        cores = list()
        gpus  = list()
        lfs   = 0

        # first count the number of free cores, gpus, and local file storage.
        # This is way quicker than actually finding the core IDs.
        free_cores = 0
        free_gpus  = 0
        free_lfs   = node['lfs']['size']
        free_cores_per_socket = list()
        free_gpus_per_socket  = list()

        for socket in node['sockets']:
            free_cores += socket['cores'].count(rpc.FREE)
            free_gpus  += socket['gpus'].count(rpc.FREE)

            free_cores_per_socket.append(socket['cores'].count(rpc.FREE))
            free_gpus_per_socket.append(socket['gpus'].count(rpc.FREE))


        alloc_lfs   = 0
        alloc_cores = 0
        alloc_gpus  = 0

        if partial:
            # For partial requests the check simplifies: we just check if we
            # have either, some cores *or* gpus *or* local_fs, to serve the
            # request
            if (requested_cores and not free_cores) and \
               (requested_gpus  and not free_gpus)  and \
               (requested_lfs   and not free_lfs):
                return [], [], None

            if requested_lfs and \
                ((requested_cores and not free_cores) and
                 (requested_gpus  and not free_gpus)):
                return [], [], None

        else:
            # For non-partial requests (ie. full requests): its a no-match if
            # either the cpu or gpu request cannot be served.
            if  requested_cores > free_cores or \
                requested_gpus  > free_gpus  or \
                requested_lfs   > free_lfs   :
                return [], [], None

        # We can serve the partial or full request - alloc the chunks we need
        # FIXME: chunk gpus, too?
        # We need to land enough procs on a node such that the cores,
        # lfs and gpus requested per proc is available on the same node

        num_procs = dict()

        if requested_lfs:
            alloc_lfs = min(requested_lfs, free_lfs)
            num_procs['lfs'] = alloc_lfs / lfs_chunk

        if requested_cores:
            self._log.debug('req cores: %s of %s [%s]', requested_cores,
                    free_cores, core_chunk)

            if self._cross_socket_threads:
                alloc_cores = min(requested_cores, free_cores)
                num_procs['cores'] = alloc_cores / core_chunk

            else:
                # If no cross socket threads are allowed, we first make sure that the total
                # number of cores is greater than the core_chunk. If not, we can't use the
                # current node.
                if sum(free_cores_per_socket) < core_chunk:
                    return [], [], None

                # If we can use this node, then we visit each socket of the node and
                # determine the number of continuous core_chunks that can be use on the
                # socket till we either run out of continuous cores or have acquired
                # the requested number of cores.
                usable_cores = 0

                # Determine maximum procs on each socket - required during assignment
                max_procs_on_socket = [0 for _ in range(self._rm_sockets_per_node)]
                self._log.debug('mpos %s', max_procs_on_socket)

                for socket_id, free_cores in enumerate(free_cores_per_socket):
                    tmp_num_cores = free_cores

                    while tmp_num_cores >= core_chunk and \
                          usable_cores  <  requested_cores:

                        usable_cores  += core_chunk
                        tmp_num_cores -= core_chunk
                        max_procs_on_socket[socket_id] += 1

                    if usable_cores == requested_cores:
                        break

                # We convert the number of usable cores into a number of procs so that
                # we can find the minimum number of procs across lfs, cpus, gpus that can
                # be allocated on this node
                num_procs['cores'] = usable_cores / core_chunk

        if requested_gpus:
            alloc_gpus = min(requested_gpus, free_gpus)
            num_procs['gpus'] = alloc_gpus / gpu_chunk

        # Find normalized lfs, cores and gpus
        if requested_cores: alloc_cores = num_procs['cores'] * core_chunk
        if requested_gpus : alloc_gpus  = num_procs['gpus']  * gpu_chunk
        if requested_lfs  : alloc_lfs   = num_procs['lfs']   * lfs_chunk
        self._log.debug('alc : %s %s %s', alloc_cores, alloc_gpus, alloc_lfs)

        # Maximum number of processes allocatable on a socket
        if self._cross_socket_threads:
            max_procs_on_socket = [num_procs['cores']
                                   for _ in range(self._rm_sockets_per_node)]
            self._log.debug('max_procs_on_socket %s', max_procs_on_socket)

        # now dig out the core IDs.
        for socket_idx, socket in enumerate(node['sockets']):
            procs_on_socket = 0
            for core_idx, state in enumerate(socket['cores']):

                # break if we have enough cores, else continue to pick FREE ones
                if alloc_cores == len(cores):
                    break
                if state == rpc.FREE:
                    cores.append(socket_idx * self._rm_cores_per_socket
                                            + core_idx)

                # check if we have placed one complete process on current socket
                if cores and (len(cores) % core_chunk == 0):
                    procs_on_socket += 1

                # if we have reached max procs on socket, move to next socket
                if procs_on_socket == max_procs_on_socket[socket_idx]:
                    break

            # break if we have enough cores, else continue to pick FREE ones
            if alloc_cores == len(cores):
                break

        # now dig out the GPU IDs.
        for socket_idx, socket in enumerate(node['sockets']):
            for gpu_idx, state in enumerate(socket['gpus']):

                # break if we have enough gpus, else continue to pick FREE ones
                if alloc_gpus == len(gpus):
                    break
                if state == rpc.FREE:
                    gpus.append(socket_idx*self._rm_gpus_per_socket+ gpu_idx)

            # break if we have enough gpus, else continue to pick FREE ones
            if alloc_gpus == len(gpus):
                break

        return cores, gpus, alloc_lfs


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

        core_map = list()
        gpu_map  = list()

        # make sure the core sets can host the requested number of threads
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
        requested_procs  = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus   = cud['gpu_processes']
        requested_lfs    = cud['lfs_per_process']
        lfs_chunk        = requested_lfs if requested_lfs > 0 else 1

        # make sure that processes are at least single-threaded
        if not threads_per_proc:
            threads_per_proc = 1

        # cores needed for all threads and processes
        requested_cores = requested_procs * threads_per_proc

        if not self._cross_socket_threads:
            if threads_per_proc > self._rm_cores_per_socket:
                raise ValueError('cu does not fit on socket')

        cores_per_node = self._rm_cores_per_socket * self._rm_sockets_per_node
        gpus_per_node  = self._rm_gpus_per_socket  * self._rm_sockets_per_node
        lfs_per_node   = self._rm_lfs_per_node['size']

        # make sure that the requested allocation fits within the resources
        if  requested_cores > cores_per_node or \
            requested_gpus  > gpus_per_node  or \
            requested_lfs   > lfs_per_node   :

            txt  = 'Non-mpi unit %s does not fit onto node \n' % uid
            txt += '   cores: %s >? %s \n' % (requested_cores, cores_per_node)
            txt += '   gpus : %s >? %s \n' % (requested_gpus,  gpus_per_node)
            txt += '   lfs  : %s >? %s'    % (requested_lfs,   lfs_per_node)
            raise ValueError(txt)

        # ok, we can go ahead and try to find a matching node
        cores     = list()
        gpus      = list()
        lfs       = None
        node_name = None
        node_uid  = None
        tag       = cud.get('tag')

        for node in self.nodes:  # FIXME optimization: iteration start

            # If unit has a tag, check if the tag is in the tag_history dict,
            # else it is a invalid tag, continue as if the unit does not have
            # a tag
            # If the unit has a valid tag, find the node that matches the
            # tag from tag_history dict
            if tag and tag in self._tag_history:
                if node['uid'] not in self._tag_history[tag]:
                    continue

            # attempt to find the required number of cores and gpus on this
            # node - do not allow partial matches.
            cores, gpus, lfs = self._find_resources(node=node,
                                                    requested_cores=requested_cores,
                                                    requested_gpus=requested_gpus,
                                                    requested_lfs=requested_lfs,
                                                    partial=False,
                                                    lfs_chunk=lfs_chunk,
                                                    core_chunk = threads_per_proc
                                                    )
            if  len(cores) == requested_cores and \
                len(gpus)  == requested_gpus:

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
        core_map, gpu_map = self._get_node_maps(cores, gpus, threads_per_proc)

        # We need to specify the node lfs path that the unit needs to use.
        # We set it as an environment variable that gets loaded with cud
        # executable.
        # Assumption enforced: The LFS path is the same across all nodes.
        cud['environment']['NODE_LFS_PATH'] = self._rm_lfs_per_node['path']

        # all the information for placing the unit is acquired - return them
        slots = {'nodes': [{'name'    : node_name,
                            'uid'     : node_uid,
                            'core_map': core_map,
                            'gpu_map' : gpu_map,
                            'lfs'     : {'size': lfs,
                                         'path': self._rm_lfs_per_node['path']
                                        }
                           }],
                 'cores_per_node': cores_per_node,
                 'gpus_per_node' : gpus_per_node,
                 'lfs_per_node'  : lfs_per_node,
                 'lm_info'       : self._rm_lm_info
                 }

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

        uid = unit['uid']
        cud = unit['description']

        # dig out the allocation request details
        requested_procs  = cud['cpu_processes']
        threads_per_proc = cud['cpu_threads']
        requested_gpus   = cud['gpu_processes']
        requested_lfs_per_process = cud['lfs_per_process']

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

        cores_per_node = self._rm_cores_per_socket * self._rm_sockets_per_node
        gpus_per_node  = self._rm_gpus_per_socket  * self._rm_sockets_per_node
        lfs_per_node   = self._rm_lfs_per_node

        if not self._cross_socket_threads:
            if threads_per_proc > self._rm_cores_per_socket:
                raise ValueError('Number of threads greater than that available on a socket')

        # we always fail when too many threads are requested
        if threads_per_proc > cores_per_node:
            raise ValueError('too many threads requested')

        if requested_lfs_per_process > lfs_per_node['size']:
            raise ValueError('Not enough LFS for the MPI-process')


        # set conditions to find the first matching node
        is_first      = True
        is_last       = False
        alloced_cores = 0
        alloced_gpus  = 0
        alloced_lfs   = 0

        slots = {'nodes': list(),
                 'cores_per_node': cores_per_node,
                 'gpus_per_node' : gpus_per_node,
                 'lfs_per_node'  : lfs_per_node,
                 'lm_info'       : self._rm_lm_info,
                }

        tag = cud.get('tag')

        # start the search
        for node in self.nodes:

            node_uid  = node['uid']
            node_name = node['name']

            # If unit has a tag, check if the tag is in the tag_history dict,
            # else it is a invalid tag, continue as if the unit does not have
            # a tag
            # If the unit has a valid tag, find the node that matches the
            # tag from tag_history dict
            if tag and tag in self._tag_history:
                if node['uid'] not in self._tag_history[tag]:
                    continue

            # if only a small set of cores/gpus remains unallocated (ie. less
            # than node size), we are in fact looking for the last node.  Note
            # that this can also be the first node, for small units.
            if  requested_cores - alloced_cores <= cores_per_node and \
                requested_gpus  - alloced_gpus  <= gpus_per_node  and \
                requested_lfs   - alloced_lfs   <= lfs_per_node['size']:
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
            find_lfs   = min(requested_lfs   - alloced_lfs,   lfs_per_node['size'])

            # under the constraints so derived, check what we find on this node
            cores, gpus, lfs = self._find_resources(node=node,
                                                    requested_cores=find_cores,
                                                    requested_gpus=find_gpus,
                                                    requested_lfs=find_lfs,
                                                    core_chunk=threads_per_proc,
                                                    partial=partial,
                                                    lfs_chunk=requested_lfs_per_process)

            # Skip nodes that provide only lfs and no cores
            if not cores and lfs:
                continue

            if not cores and not gpus and not lfs:

                # this was not a match. If we are in  'scattered' mode, we just
                # ignore this node.  Otherwise we have to restart the search.
                if not self._scattered:
                    is_first      = True
                    is_last       = False
                    alloced_cores = 0
                    alloced_gpus  = 0
                    alloced_lfs   = 0
                    slots['nodes'] = list()

                # try next node
                continue

            # we found something - add to the existing allocation, switch gears
            # (not first anymore), and try to find more if needed
            self._log.debug('found %s cores, %s gpus, %s lfs', cores, gpus, lfs)
            core_map, gpu_map = self._get_node_maps(cores, gpus, threads_per_proc)

            # We need to specify the node lfs path that the unit needs to use.
            # We set it as an environment variable that gets loaded with cud
            # executable.
            # Assumption enforced: The LFS path is the same across all nodes.
            lfs_path = self._rm_lfs_per_node['path']
            if 'NODE_LFS_PATH' not in cud['environment']:
                cud['environment']['NODE_LFS_PATH'] = lfs_path

            slots['nodes'].append({'name'    : node_name,
                                   'uid'     : node_uid,
                                   'core_map': core_map,
                                   'gpu_map' : gpu_map,
                                   'lfs'     : {'size': lfs,
                                                'path': lfs_path}})

            alloced_cores += len(cores)
            alloced_gpus  += len(gpus)
            alloced_lfs   += lfs

            is_first = False

            # or maybe don't continue the search if we have in fact enough!
            if  alloced_cores == requested_cores and \
                alloced_gpus  == requested_gpus  and \
                alloced_lfs   == requested_lfs:
                # we are done
                break

        # if we did not find enough, there is not much we can do at this point
        if  alloced_cores < requested_cores or \
            alloced_gpus  < requested_gpus  or \
            alloced_lfs   < requested_lfs:
            return None  # signal failure

        # this should be nicely filled out now - return
        return slots


# ------------------------------------------------------------------------------

