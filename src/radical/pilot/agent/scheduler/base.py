
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time
import queue
import logging

import multiprocessing    as mp

import radical.utils      as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
#
# 'enum' for RPs's pilot scheduler types
#
SCHEDULER_NAME_CONTINUOUS_ORDERED = "CONTINUOUS_ORDERED"
SCHEDULER_NAME_CONTINUOUS_COLO    = "CONTINUOUS_COLO"
SCHEDULER_NAME_CONTINUOUS         = "CONTINUOUS"
SCHEDULER_NAME_HOMBRE             = "HOMBRE"
SCHEDULER_NAME_FLUX               = "FLUX"
SCHEDULER_NAME_TORUS              = "TORUS"
SCHEDULER_NAME_NOOP               = "NOOP"
SCHEDULER_NAME_TORUS              = "TORUS"

# SCHEDULER_NAME_YARN               = "YARN"
# SCHEDULER_NAME_SPARK              = "SPARK"
# SCHEDULER_NAME_CONTINUOUS_SUMMIT  = "CONTINUOUS_SUMMIT"
# SCHEDULER_NAME_CONTINUOUS_FIFO    = "CONTINUOUS_FIFO"
# SCHEDULER_NAME_SCATTERED          = "SCATTERED"


# ------------------------------------------------------------------------------
#
# An RP agent scheduler will place incoming tasks onto a set of cores and gpus.
#
# This is the agent scheduler base class.  It provides the framework for
# implementing diverse scheduling algorithms and mechanisms, tailored toward
# specific workload types, resource configurations, batch systems etc.
#
# The base class provides the following functionality to the implementations:
#
#   - obtain configuration settings from config files and environments
#   - create aself._nodes list to represent available resources;
#   - general control and data flow:
#
#       # main loop
#       self._handle_task(task):  # task arrives
#         try_allocation(task)    # placement is attempted
#         if success:
#            advance(task)        # pass task to executor
#         else:
#            wait.append(task)    # place task in a wait list
#
#   - notification management:
#     - the scheduler receives notifications about tasks which completed
#       execution, and whose resources can now be used again for other tasks,
#     - the above triggers an 'unschedule' (free resources) action and also a
#       `schedule` action (check waitpool if waiting tasks can now be placed).
#
#
# A scheduler implementation will derive from this base class, and overload the
# following three methods:
#
#   _configure():
#     - make sure that the base class configuration is usable
#     - do any additional configuration
#
#   _schecule_task(task):
#     - given a task (incl. description), find and return a suitable allocation
#
#   unschedule_task(task):
#     - release the allocation held by that task
#
#
# The scheduler needs (in the general case) three pieces of information:
#
#   - the layout of the resource (nodes, cores, gpus);
#   - the current state of those (what cores/gpus are used by other tasks)
#   - the requirements of the task (single/multi node, cores, gpus)
#
# The first part (layout) is provided by the ResourceManager, in the form of a nodelist:
#
#    nodelist = [{name : 'node_1', cores: 16, gpus : 2},
#                {name : 'node_2', cores: 16, gpus : 2},
#                ...
#               ]
#
# That is then mapped into an internal representation, which is really the same
# but allows to keep track of resource usage, by setting the fields to
# `rpc.FREE == '-'` or `rpc.BUSY == '#'`:
#
#    nodelist = [{name : 'node_1', cores: [----------------], gpus : [--]},
#                {name : 'node_2', cores: {----------------], gpus : [--]},
#                ...
#               ]
#
# When allocating a set of resource for a task (2 cores, 1 gpu), we can now
# record those as used:
#
#    nodelist = [{name : 'node_1', cores: [##--------------], gpus : [#-]},
#                {name : 'node_2', cores: {----------------], gpus : [--]},
#                ...
#               ]
#
# This solves the second part from our list above.  The third part, task
# requirements, are obtained from the task dict passed for scheduling: the task
# description contains requests for `cores` and `gpus`.
#
# Note that the task description will also list the number of processes and
# threads for cores and gpus, and also flags the use of `mpi`, 'openmp', etc.
# The scheduler will have to make sure that for each process to be placed, the
# given number of additional cores are available *and reserved* to create
# threads on.  The threads are created by the application.  Note though that
# this implies that the launcher must pin cores very carefully, to not constrain
# the thread creation for the application.
#
# The scheduler algorithm will then attempt to find a suitable set of cores and
# gpus in the nodelist into which the task can be placed.  It will mark those as
# `rpc.BUSY`, and attach the set of cores/gpus to the task dictionary, as (here
# for system with 8 cores & 1 gpu per node):
#
#     task = { ...
#       'cpu_processes'   : 4,
#       'cpu_process_type': 'mpi',
#       'cpu_threads'     : 2,
#       'gpu_processes    : 2,
#       'slots' :
#       {                 # [[node,   node_uid,   [cpu idx],        [gpu idx]]]
#         'nodes'         : [[node_1, node_uid_1, [[0, 2], [4, 6]], [[0]    ]],
#                            [node_2, node_uid_2, [[1, 3], [5, 7]], [[0]    ]]],
#         'cores_per_node': 8,
#         'gpus_per_node' : 1,
#         'lm_info'       : { ... }
#       }
#     }
#
# The `cpu idx` field is a list of sets, where in each set the first core is
# where an application process is places, while the other cores are reserved for
# that process' threads.  For GPUs we use the same structure, but GPU processes
# are currently all considered to be single-threaded.
#
# The respective launch method is expected to create processes on the set of
# cpus and gpus thus specified, (node_1, cores 0 and 4; node_2, cores 1 and 5).
# The other reserved cores are for the application to spawn threads on
# (`cpu_threads=2`).
#
# A scheduler MAY attach other information to the `slots` structure, with the
# intent to support the launch methods to enact the placement decition made by
# the scheduler.  In fact, a scheduler may use a completely different slot
# structure than above - but then is likely bound to a specific launch method
# which can interpret that structure.  A notable example is the BG/Q torus
# scheduler which will only work in combination with the dplace launch methods.
# `lm_info` is an opaque field which allows to communicate specific settings
# from the rm to the launch method.
#
# FIXME: `lm_info` should be communicated to the LM instances in creation, not
#        as part of the slots.  Its constant anyway, as lm_info is set only
#        once during rm startup.
#
# NOTE:  While the nodelist resources are listed as strings above, we in fact
#        use a list of integers, to simplify some operations, and to
#        specifically avoid string copies on manipulations.  We only convert
#        to a stringlist for visual representation (`self.slot_status()`).
#
# NOTE:  The scheduler will allocate one core per node and GPU, as some startup
#        methods only allow process placements to *cores*, even if GPUs are
#        present and requested (hi aprun).  We should make this decision
#        dependent on the `lm_info` field - but at this point we don't have this
#        information (at least not readily available).
#
# TODO:  use named tuples for the slot structure to make the code more readable,
#        specifically for the LaunchMethods.
#
# NOTE:  The set of profiler events generated by this component are:
#
#        schedule_try    : search for task resources starts    (uid: uid)
#        schedule_fail   : search for task resources failed    (uid: uid)
#        schedule_ok     : search for task resources succeeded (uid: uid)
#        unschedule_start: task resource freeing starts        (uid: uid)
#        unschedule_stop : task resource freeing stops         (uid: uid)
#
#        See also:
#        https://github.com/radical-cybertools/radical.pilot/blob/feature/ \
#                           events/docs/source/events.md \
#                           #agentschedulingcomponent-component


# ------------------------------------------------------------------------------
#
class AgentSchedulingComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    # the deriving schedulers should in general have the following structure in
    # self.nodes:
    #
    #   self.nodes = [
    #     { 'name'  : 'name-of-node',
    #       'uid'   : 'uid-of-node',
    #       'cores' : '###---##-##-----',  # 16 cores, free/busy markers
    #       'gpus'  : '--',                #  2 GPUs,  free/busy markers
    #     }, ...
    #   ]
    #
    # The free/busy markers are defined in rp.constants.py, and are `-` and `#`,
    # respectively.  Some schedulers may need a more elaborate structures - but
    # where the above is suitable, it should be used for code consistency.
    #

    def __init__(self, cfg, session):

        self.nodes = None
        self._uid  = ru.generate_id(cfg['owner'] + '.scheduling.%(counter)s',
                                    ru.ID_CUSTOM)
        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize()` will be called
    # before control is given to the component's main loop.
    #
    def initialize(self):

        # The scheduler needs the ResourceManager information which have been collected
        # during agent startup.  We dig them out of the config at this point.
        #
        # NOTE: this information is insufficient for the torus scheduler!
        self._pid               = self._cfg['pid']
        self._rm_info           = self._cfg['rm_info']
        self._rm_lm_info        = self._cfg['rm_info']['lm_info']
        self._rm_node_list      = self._cfg['rm_info']['node_list']
        self._rm_partitions     = self._cfg['rm_info']['partitions']
        self._rm_cores_per_node = self._cfg['rm_info']['cores_per_node']
        self._rm_gpus_per_node  = self._cfg['rm_info']['gpus_per_node']
        self._rm_lfs_per_node   = self._cfg['rm_info']['lfs_per_node']
        self._rm_mem_per_node   = self._cfg['rm_info']['mem_per_node']

        if not self._rm_node_list:
            raise RuntimeError("ResourceManager %s didn't _configure node_list."
                              % self._rm_info['name'])

        if self._rm_cores_per_node is None:
            raise RuntimeError("ResourceManager %s didn't _configure cores_per_node."
                              % self._rm_info['name'])

        if self._rm_gpus_per_node is None:
            raise RuntimeError("ResourceManager %s didn't _configure gpus_per_node."
                              % self._rm_info['name'])

        # create and initialize the wait pool.  Also maintain a mapping of that
        # waitlist to a binned list where tasks are binned by size for faster
        # lookups of replacement tasks.  And outdated binlist is mostly
        # sufficient, only rebuild when we run dry
        self._waitpool = dict()  # map uid:task
        self._ts_map   = dict()
        self._ts_valid = False  # set to False to trigger re-binning

        # the scheduler algorithms have two inputs: tasks to be scheduled, and
        # slots becoming available (after tasks complete).
        self._queue_sched   = mp.Queue()
        self._queue_unsched = mp.Queue()
        self._proc_term     = mp.Event()  # signal termination ot scheduler proc

        # initialize the node list to be used by the scheduler.  A scheduler
        # instance may decide to overwrite or extend this structure.
        self.nodes = list()
        for node, node_uid in self._rm_node_list:
            self.nodes.append({'uid'  : node_uid,
                               'name' : node,
                               'cores': [rpc.FREE] * self._rm_cores_per_node,
                               'gpus' : [rpc.FREE] * self._rm_gpus_per_node,
                               'lfs'  :              self._rm_lfs_per_node,
                               'mem'  :              self._rm_mem_per_node})

        # configure the scheduler instance
        self._configure()
        self.slot_status("slot status after  init")

        # register task input channels
        self.register_input(rps.AGENT_SCHEDULING_PENDING,
                            rpc.AGENT_SCHEDULING_QUEUE, self.work)

        # we need unschedule updates to learn about tasks for which to free the
        # allocated cores.  Those updates MUST be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.register_subscriber(rpc.AGENT_UNSCHEDULE_PUBSUB, self.unschedule_cb)

        # start a process to host the actual scheduling algorithm
        self._p = mp.Process(target=self._schedule_tasks)
        self._p.daemon = True
        self._p.start()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._p.terminate()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        raise NotImplementedError('deriving classes must implement this')


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate instance for the scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # make sure that we are the base-class!
        if cls != AgentSchedulingComponent:
            raise TypeError("Scheduler Factory only available to base class!")

        name = cfg['scheduler']

        from .continuous_ordered import ContinuousOrdered
        from .continuous_colo    import ContinuousColo
        from .continuous         import Continuous
        from .hombre             import Hombre
        from .flux               import Flux
        from .torus              import Torus
        from .noop               import Noop

      # from .yarn               import Yarn
      # from .spark              import Spark
      # from .continuous_summit  import ContinuousSummit
      # from .continuous_fifo    import ContinuousFifo
      # from .scattered          import Scattered

        try:
            impl = {

                SCHEDULER_NAME_CONTINUOUS_ORDERED : ContinuousOrdered,
                SCHEDULER_NAME_CONTINUOUS_COLO    : ContinuousColo,
                SCHEDULER_NAME_CONTINUOUS         : Continuous,
                SCHEDULER_NAME_HOMBRE             : Hombre,
                SCHEDULER_NAME_FLUX               : Flux,
                SCHEDULER_NAME_TORUS              : Torus,
                SCHEDULER_NAME_NOOP               : Noop,

              # SCHEDULER_NAME_YARN               : Yarn,
              # SCHEDULER_NAME_SPARK              : Spark,
              # SCHEDULER_NAME_CONTINUOUS_SUMMIT  : ContinuousSummit,
              # SCHEDULER_NAME_CONTINUOUS_FIFO    : ContinuousFifo,
              # SCHEDULER_NAME_SCATTERED          : Scattered,

            }[name]

            impl = impl(cfg, session)
            return impl

        except KeyError as e:
            raise ValueError("Scheduler '%s' unknown or defunct" % name) from e


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

        # for node_name, node_uid, cores, gpus in slots['nodes']:
        for slot_node in slots['nodes']:

            # Find the entry in the the slots list

            # TODO: [Optimization] Assuming 'uid' is the ID of the node, it
            #       seems a bit wasteful to have to look at all of the nodes
            #       available for use if at most one node can have that uid.
            #       Maybe it would be worthwhile to simply keep a list of nodes
            #       that we would read, and keep a dictionary that maps the uid
            #       of the node to the location on the list?

            node = None
            node_found = False
            for node in self.nodes:
                if node['uid'] == slot_node['uid']:
                    node_found = True
                    break

            if not node_found:
                raise RuntimeError('inconsistent node information')

            # iterate over cores/gpus in the slot, and update state
            cores = slot_node['core_map']
            for cslot in cores:
                for core in cslot:
                    node['cores'][core] = new_state

            gpus = slot_node['gpu_map']
            for gslot in gpus:
                for gpu in gslot:
                    node['gpus'][gpu] = new_state

            if slot_node['lfs']['path']:
                if new_state == rpc.BUSY:
                    node['lfs']['size'] -= slot_node['lfs']['size']
                else:
                    node['lfs']['size'] += slot_node['lfs']['size']

            if slot_node['mem']:
                if new_state == rpc.BUSY:
                    node['mem'] -= slot_node['mem']
                else:
                    node['mem'] += slot_node['mem']



    # --------------------------------------------------------------------------
    #
    # NOTE: any scheduler implementation which uses a different nodelist
    #       structure MUST overload this method.
    def slot_status(self, msg=None):
        '''
        Returns a multi-line string corresponding to the status of the node list
        '''

        if not self._log.isEnabledFor(logging.DEBUG):
            return

        if not msg: msg = ''

        glyphs = {rpc.FREE : '-',
                  rpc.BUSY : '#',
                  rpc.DOWN : '!'}
        ret = "|"
        for node in self.nodes:
            for core in node['cores']:
                ret += glyphs[core]
            ret += ':'
            for gpu in node['gpus']:
                ret += glyphs[gpu]
            ret += '|'

        self._log.debug("status: %-30s: %s", msg, ret)

        return ret


    # --------------------------------------------------------------------------
    #
    def _refresh_ts_map(self):

        # The ts map only gets invalidated when new tasks get added to the
        # waitpool.  Removing tasks does *not* invalidate it.
        #
        # This method should only be called opportunistically, i.e., when a task
        # lookup failed and it is worthwhile checking the waitlist tasks.

        if self._ts_valid:
            # nothing to do, the map is valid
            return

        # we can only rebuild if we have waiting tasks
        if not self._waitpool:
            return

        for uid,task in self._waitpool.items():
            ts = task['tuple_size']
            if ts not in self._ts_map:
                self._ts_map[ts] = set()
            self._ts_map[ts].add(uid)

        self._ts_valid = True


    # --------------------------------------------------------------------------
    #
    def schedule_task(self, task):

        raise NotImplementedError('schedule_task needs to be implemented.')


    # --------------------------------------------------------------------------
    #
    def unschedule_task(self, task):

        raise NotImplementedError('unschedule_task needs to be implemented.')


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):
        '''
        This is the main callback of the component, which is called for any
        incoming (set of) task(s).  Tasks arriving here must always be in
        `AGENT_SCHEDULING_PENDING` state, and must always leave in either
        `AGENT_EXECUTING_PENDING` or in a FINAL state (`FAILED` or `CANCELED`).
        While handled by this component, the tasks will be in `AGENT_SCHEDULING`
        state.

        This methods takes care of initial state change to `AGENT_SCHEDULING`,
        and then puts them forward onto the queue towards the actual scheduling
        process (self._schedule_tasks).
        '''

        # unify handling of bulks / non-bulks
        tasks = ru.as_list(tasks)

        # advance state, publish state change, and push to scheduler process
        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)
        self._queue_sched.put(tasks)


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, msg):
        '''
        release (for whatever reason) all slots allocated to this task
        '''

        self._queue_unsched.put(msg)

        # return True to keep the cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def _schedule_tasks(self):
        '''
        This method runs in a separate process and hosts the actual scheduling
        algorithm invocation.  The process is fed by two queues: a queue of
        incoming tasks to schedule, and a queue of slots freed by finishing
        tasks.
        '''

        # The loop alternates between
        #
        #   - scheduling tasks from a waitpool;
        #   - pulling new tasks to schedule; and
        #   - pulling for free slots to use.
        #
        # resources = True  # fresh start
        #
        # while True:
        #
        #   if resources:  # otherwise: why bother?
        #     if waitpool:
        #       sort(waitpool)  # largest tasks first
        #       for task in sorted(waitpool):
        #         if task >= max_task:
        #           prof schedule_skip
        #         else:
        #           if try_schedule:
        #             advance
        #             continue
        #           max_task = max(max_task, size(task))
        #         break  # larger tasks won't work
        #
        #   if any_resources:  # otherwise: why bother
        #     for task in queue_tasks.get():
        #         if task <= max_task:
        #           if try_schedule:
        #             advance
        #             continue
        #           waitpool.append(task)
        #           max_task = max(max_task, size(task))
        #         continue  # next task mght be smaller
        #
        #   resources = False  # nothing in waitpool fits
        #   for slot in queue_slots.get():
        #     free_slot(slot)
        #     resources = True  # maybe we can place a task now


        # register task output channels
        self.register_output(rps.AGENT_EXECUTING_PENDING,
                             rpc.AGENT_EXECUTING_QUEUE)

        resources = True  # fresh start, all is free
        while not self._proc_term.is_set():

          # self._log.debug('=== schedule tasks 0: %s, w: %d', resources,
          #         len(self._waitpool))

            active = 0  # see if we do anything in this iteration

            # if we have new resources, try to place waiting tasks.
            r_wait = False
            if resources:
                r_wait, a = self._schedule_waitpool()
                active += int(a)
              # self._log.debug('=== schedule tasks w: %s %s', r_wait, a)

            # always try to schedule newly incoming tasks
            # running out of resources for incoming could still mean we have
            # smaller slots for waiting tasks, so ignore `r` for now.
            r_inc, a = self._schedule_incoming()
            active += int(a)
          # self._log.debug('=== schedule tasks i: %s %s', r_inc, a)

            # if we had resources, but could not schedule any incoming not any
            # waiting, then we effectively ran out of *useful* resources
            if resources and (r_wait is False and r_inc is False):
                resources = False

            # reclaim resources from completed tasks
            # if tasks got unscheduled (and not replaced), then we have new
            # space to schedule waiting tasks (unless we have resources from
            # before)
            r, a = self._unschedule_completed()
            if not resources and r:
                resources = True
            active += int(a)
          # self._log.debug('=== schedule tasks c: %s %s', r, a)

            if not active:
                time.sleep(0.1)  # FIXME: configurable

          # self._log.debug('=== schedule tasks x: %s %s', resources, active)


    # --------------------------------------------------------------------------
    #
    def _prof_sched_skip(self, task):

        pass
      # self._prof.prof('schedule_skip', uid=task['uid'])


    # --------------------------------------------------------------------------
    #
    def _schedule_waitpool(self):

      # self.slot_status("before schedule waitpool")

        # sort by inverse tuple size to place larger tasks first and backfill
        # with smaller tasks.  We only look at cores right now - this needs
        # fixing for GPU dominated loads.
        # We define `tuple_size` as
        #     `(cpu_processes + gpu_processes) * cpu_threads`
        #
        tasks = list(self._waitpool.values())
        tasks.sort(key=lambda x:
                (x['tuple_size'][0] + x['tuple_size'][2]) * x['tuple_size'][1],
                 reverse=True)

        # cycle through waitpool, and see if we get anything placed now.
        scheduled, unscheduled = ru.lazy_bisect(tasks,
                                                check=self._try_allocation,
                                                on_skip=self._prof_sched_skip,
                                                log=self._log)

        self._waitpool = {task['uid']:task for task in unscheduled}

        # update task resources
        for task in scheduled:
            td = task['description']
            task['$set']      = ['resources']
            task['resources'] = {'cpu': td['cpu_processes'] *
                                        td.get('cpu_threads', 1),
                                 'gpu': td['gpu_processes']}
        self.advance(scheduled, rps.AGENT_EXECUTING_PENDING, publish=True,
                                                             push=True)

        # method counts as `active` if anything was scheduled
        active = bool(scheduled)

        # if we sccheduled some tasks but not all, we ran out of resources
        resources = not (bool(unscheduled) and bool(unscheduled))

      # self.slot_status("after  schedule waitpool")
        return resources, active


    # --------------------------------------------------------------------------
    #
    def _schedule_incoming(self):

        # fetch all tasks from the queue
        tasks = list()
        try:

            while not self._proc_term.is_set():
                data = self._queue_sched.get(timeout=0.001)

                if not isinstance(data, list):
                    data = [data]

                for task in data:
                    self._set_tuple_size(task)
                    tasks.append(task)

        except queue.Empty:
            # no more unschedule requests
            pass

        if not tasks:
            # no resource change, no activity
            return None, False

      # self.slot_status("before schedule incoming [%d]" % len(tasks))

        # handle largest tasks first
        # FIXME: this needs lazy-bisect
        to_wait = list()
        for task in sorted(tasks, key=lambda x: x['tuple_size'][0],
                           reverse=True):

            # either we can place the task straight away, or we have to
            # put it in the wait pool.
            if self._try_allocation(task):

                # task got scheduled - advance state, notify world about the
                # state change, and push it out toward the next component.
                td = task['description']
                task['$set']      = ['resources']
                task['resources'] = {'cpu': td['cpu_processes'] *
                                            td.get('cpu_threads', 1),
                                     'gpu': td['gpu_processes']}
                self.advance(task, rps.AGENT_EXECUTING_PENDING,
                             publish=True, push=True)

            else:
                to_wait.append(task)

        # all tasks which could not be scheduled are added to the waitpool
        self._waitpool.update({task['uid']:task for task in to_wait})

        # we performed some activity (worked on tasks)
        active = True

        # if tasks remain waiting, we are out of usable resources
        resources = not bool(to_wait)

        # incoming tasks which have to wait are the only reason to rebuild the
        # tuple_size map
        self._ts_valid = False

      # self.slot_status("after  schedule incoming")
        return resources, active


    # --------------------------------------------------------------------------
    #
    def _unschedule_completed(self):

        to_unschedule = list()
        try:

            # Timeout and bulk limit below are somewhat arbitrary, but the
            # behaviour is benign.  The goal is to avoid corner cases: for the
            # sleep, avoid no sleep (busy idle) and also significant latencies.
            # Anything smaller than 0.01 is under our noise level and works ok
            # for the latency, and anything larger than 0 is sufficient to avoid
            # busy idle.
            #
            # For the unschedule bulk, the corner case to avoid is waiting for
            # too long to fill a bulk so that latencies add a up and negate the
            # bulk optimization. For the 0.001 sleep, 128 as bulk size results
            # in a max added latency of about 0.1 second, which is one order of
            # magnitude above our noise level again and thus acceptable (tm).
            while not self._proc_term.is_set():
                task = self._queue_unsched.get(timeout=0.001)
                to_unschedule.append(task)
                if len(to_unschedule) > 128:
                    break

        except queue.Empty:
            # no more unschedule requests
            pass

        to_release = list()  # slots of unscheduling tasks
        placed     = list()  # uids of waiting tasks replacing unscheduled ones

        if to_unschedule:

            # rebuild the tuple_size binning, maybe
            self._refresh_ts_map()


        for task in to_unschedule:
            # if we find a waiting task with the same tuple size, we don't free
            # the slots, but just pass them on unchanged to the waiting task.
            # Thus we replace the unscheduled task on the same cores / GPUs
            # immediately. This assumes that the `tuple_size` is good enough to
            # judge the legality of the resources for the new target task.
            #
            # FIXME

          # ts = tuple(task['tuple_size'])
          # if self._ts_map.get(ts):
          #
          #     replace = self._waitpool[self._ts_map[ts].pop()]
          #     replace['slots'] = task['slots']
          #     placed.append(placed)
          #
          #     # unschedule task A and schedule task B have the same
          #     # timestamp
          #     ts = time.time()
          #     self._prof.prof('unschedule_stop', uid=task['uid'],
          #                     timestamp=ts)
          #     self._prof.prof('schedule_fast', uid=replace['uid'],
          #                     timestamp=ts)
          #     self.advance(replace, rps.AGENT_EXECUTING_PENDING,
          #                  publish=True, push=True)
          # else:
          #
          #     # no replacement task found: free the slots, and try to
          #     # schedule other tasks of other sizes.
          #     to_release.append(task)

            to_release.append(task)

        if not to_release:
            if not to_unschedule:
                # no new resources, not been active
                return False, False
            else:
                # no new resources, but activity
                return False, True

        # we have tasks to unschedule, which will free some resources. We can
        # thus try to schedule larger tasks again, and also inform the caller
        # about resource availability.
        for task in to_release:
            self.unschedule_task(task)
            self._prof.prof('unschedule_stop', uid=task['uid'])

        # we placed some previously waiting tasks, and need to remove those from
        # the waitpool
        self._waitpool = {task['uid']:task for task in self._waitpool.values()
                                           if  task['uid'] not in placed}

        # we have new resources, and were active
        return True, True


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, task):
        '''
        attempt to allocate cores/gpus for a specific task.
        '''

        uid = task['uid']
      # self._prof.prof('schedule_try', uid=uid)

        slots = self.schedule_task(task)
        if not slots:

            # schedule failure
          # self._prof.prof('schedule_fail', uid=uid)
            return False

        # the task was placed, we need to reflect the allocation in the
        # nodelist state (BUSY) and pass placement to the task, to have
        # it enacted by the executor
        self._change_slot_states(slots, rpc.BUSY)
        task['slots'] = slots

        if slots['lfs_per_node']['path']:
            task['description']['environment']['NODE_LFS_PATH'] = \
                slots['lfs_per_node']['path']

        self._handle_cuda(task)

        # got an allocation, we can go off and launch the process
        self._prof.prof('schedule_ok', uid=uid)

        return True


    # --------------------------------------------------------------------------
    #
    def _handle_cuda(self, task):

        # Check if task requires GPUs.  If so, set CUDA_VISIBLE_DEVICES to the
        # list of assigned  GPU IDs.  We only handle uniform GPU setting for
        # now, and will isse a warning on non-uniform ones.
        #
        # The default setting is ``
        #
        # FIXME: This code should probably live elsewhere, not in this
        #        performance critical scheduler base class
        #
        # FIXME: The specification for `CUDA_VISIBLE_DEVICES` is actually Launch
        #        Method dependent.  Assume the scheduler assigns the second GPU.
        #        Manually, one would set `CVD=1`.  That also holds for launch
        #        methods like `fork` which leave GPU indexes unaltered.  Other
        #        launch methods like `jsrun` mask the system GPUs and only the
        #        second GPU is visible to the task.  To CUDA the system now
        #        seems to have only one GPU, and we need set it to `CVD=0`.
        #
        #        In other words, CVD sometimes needs to be set to the physical
        #        GPU IDs, and at other times to the logical GPU IDs (IDs as
        #        visible to the task).  This also implies that this code should
        #        actually live within the launch method.  On the upside, the
        #        Launch Method should also be able to handle heterogeneus tasks.
        #
        #        For now, we default the CVD ID mode to `physical`, thus
        #        assuming that unassigned GPUs are not masked away, as for
        #        example with `fork` and 'prte'.

        lm_info     = self._cfg['rm_info']['lm_info']
        cvd_id_mode = lm_info.get('cvd_id_mode', 'physical')

        gpu_maps = list()
        for node in task['slots']['nodes']:
            if node['gpu_map'] not in gpu_maps:
                gpu_maps.append(node['gpu_map'])

        if not gpu_maps or not gpu_maps[0]:
            # no gpu maps, nothing to do
            pass

        elif len(gpu_maps) > 1:
            # FIXME: this does not actually check for uniformity
            self._log.warn('cannot set CUDA_VISIBLE_DEVICES for non-uniform'
                           'GPU schedule (%s) - task may fail!' % gpu_maps)

        else:
            # uniform, non-zero gpu map
            gpu_map = gpu_maps[0]

            if cvd_id_mode == 'physical':
                task['description']['environment']['CUDA_VISIBLE_DEVICES'] = \
                    ','.join([str(gpu_set[0]) for gpu_set in gpu_map])

            elif cvd_id_mode == 'logical':
                task['description']['environment']['CUDA_VISIBLE_DEVICES'] = \
                    ','.join([str(x) for x in range(len(gpu_map))])

            else:
                raise ValueError('invalid CVD mode %s' % cvd_id_mode)


    # --------------------------------------------------------------------------
    #
    def _get_node_maps(self, cores, gpus, threads_per_proc):
        '''
        For a given set of cores and gpus, chunk them into sub-sets so that each
        sub-set can host one application process and all threads of that
        process.  Note that we currently consider all GPU applications to be
        single-threaded.

        example:
            cores  : [1, 2, 3, 4, 5, 6, 7, 8]
            gpus   : [1, 2]
            tpp    : 4
            result : [[1, 2, 3, 4], [5, 6, 7, 8]], [[1], [2]]

        For more details, see top level comment of `base.py`.
        '''

        core_map = list()
        gpu_map  = list()

        # make sure the core sets can host the requested number of threads
        assert(not len(cores) % threads_per_proc)
        n_procs =  int(len(cores) / threads_per_proc)

        idx = 0
        for _ in range(n_procs):
            p_map = list()
            for _ in range(threads_per_proc):
                p_map.append(cores[idx])
                idx += 1
            core_map.append(p_map)

        assert(idx == len(cores)), \
              ('%s -- %s -- %s -- %s' % idx, len(cores), cores, n_procs)

        # gpu procs are considered single threaded right now (FIXME)
        for g in gpus:
            gpu_map.append([g])

        return core_map, gpu_map


    # --------------------------------------------------------------------------
    #
    def _set_tuple_size(self, task):
        '''
        Scheduling, in very general terms, maps resource request to available
        resources.  While the scheduler may check arbitrary task attributes in
        order to estimate the resource requirements of the tast, we assume that
        the most basic attributes (cores, threads, GPUs, MPI/non-MPI) determine
        the resulting placement decision.  Specifically, we assume that this
        tuple of attributes result in a placement that is valid for all tasks
        which have the same attribute tuple.

        To speed up that tuple lookup and to simplify some scheduler
        optimizations, we extract that attribute tuple on task arrival, and will
        use it for fast task-to-placement lookups.
        '''

        d = task['description']
        task['tuple_size'] = tuple([d.get('cpu_processes', 1),
                                    d.get('cpu_threads',   1),
                                    d.get('gpu_processes', 0),
                                    d.get('cpu_process_type')])


# ------------------------------------------------------------------------------

