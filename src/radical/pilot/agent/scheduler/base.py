
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import copy
import logging
import queue
import time
import threading          as mt

import multiprocessing    as mp

import radical.utils      as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from ..resource_manager import ResourceManager


# ------------------------------------------------------------------------------
#
# 'enum' for RPs's pilot scheduler types
#
SCHEDULER_NAME_CONTINUOUS_ORDERED = "CONTINUOUS_ORDERED"
SCHEDULER_NAME_CONTINUOUS_COLO    = "CONTINUOUS_COLO"
SCHEDULER_NAME_CONTINUOUS         = "CONTINUOUS"
SCHEDULER_NAME_HOMBRE             = "HOMBRE"
SCHEDULER_NAME_FLUX               = "FLUX"
SCHEDULER_NAME_NOOP               = "NOOP"

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
#   - create self.nodes list to represent available resources;
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
#       {                 # [[node,   node_id,   [cpu map],        [gpu map]]]
#         'ranks'         : [[node_1, node_id_1, [[0, 2], [4, 6]], [[0]    ]],
#                            [node_2, node_id_2, [[1, 3], [5, 7]], [[0]    ]]],
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
# intent to support the launch methods to enact the placement decision made by
# the scheduler.  In fact, a scheduler may use a completely different slot
# structure than above - but then is likely bound to a specific launch method
# which can interpret that structure.
#
# NOTE:  The scheduler will allocate one core per node and GPU, as some startup
#        methods only allow process placements to *cores*, even if GPUs are
#        present and requested (hi aprun).  We should make this decision
#        dependent on the launch method - but at this point we don't have this
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
    #     { 'node_name' : 'name-of-node',
    #       'node_id'   : 'uid-of-node',
    #       'cores'     : '###---##-##-----',  # 16 cores, free/busy markers
    #       'gpus'      : '--',                #  2 GPUs,  free/busy markers
    #     }, ...
    #   ]
    #
    # The free/busy markers are defined in rp.constants.py, and are `-` and `#`,
    # respectively.  Some schedulers may need a more elaborate structures - but
    # where the above is suitable, it should be used for code consistency.
    #

    def __init__(self, cfg, session):

        self.nodes = []
        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize()` will be called
    # before control is given to the component's main loop.
    #
    def initialize(self):

        # The scheduler needs the ResourceManager information which have been
        # collected during agent startup.
        self._rm = ResourceManager.create(self._cfg.resource_manager,
                                          self._cfg, self._log, self._prof)

        self._partitions = self._rm.get_partitions()  # {plabel : [node_ids]}

        # create and initialize the wait pool.  Also maintain a mapping of that
        # waitlist to a binned list where tasks are binned by size for faster
        # lookups of replacement tasks.  And outdated binlist is mostly
        # sufficient, only rebuild when we run dry
        self._waitpool   = dict()  # map uid:task
        self._ts_map     = dict()
        self._ts_valid   = False   # set to False to trigger re-binning
        self._active_cnt = 0       # count of currently scheduled tasks
        self._named_envs = list()  # record available named environments

        # the scheduler algorithms have two inputs: tasks to be scheduled, and
        # slots becoming available (after tasks complete).
        self._queue_sched   = mp.Queue()
        self._queue_unsched = mp.Queue()
        self._proc_term     = mp.Event()  # signal termination of scheduler proc

        # initialize the node list to be used by the scheduler.  A scheduler
        # instance may decide to overwrite or extend this structure.
        self.nodes = copy.deepcopy(self._rm.info.node_list)

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

        self._proc_term.set()
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
        from .noop               import Noop

        impl = {

            SCHEDULER_NAME_CONTINUOUS_ORDERED : ContinuousOrdered,
            SCHEDULER_NAME_CONTINUOUS_COLO    : ContinuousColo,
            SCHEDULER_NAME_CONTINUOUS         : Continuous,
            SCHEDULER_NAME_HOMBRE             : Hombre,
            SCHEDULER_NAME_FLUX               : Flux,
            SCHEDULER_NAME_NOOP               : Noop,
        }

        if name not in impl:
            raise ValueError('Scheduler %s unknown' % name)

        return impl[name](cfg, session)


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):
        '''
        We listen on the control channel for raptor queue registration commands
        '''

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'register_named_env':

            env_name = arg['env_name']
            self._named_envs.append(env_name)


        if cmd == 'register_raptor_queue':

            name  = arg['name']
            queue = arg['queue']
            addr  = arg['addr']

            self._log.debug('register raptor queue: %s', name)

            with self._raptor_lock:

                self._raptor_queues[name] = ru.zmq.Putter(queue, addr)

                if name in self._raptor_tasks:

                    tasks = self._raptor_tasks[name]
                    del(self._raptor_tasks[name])

                    self._log.debug('relay %d tasks to raptor %d', len(tasks), name)
                    self._raptor_queues[name].put(tasks)


        elif cmd == 'unregister_raptor_queue':

            name = arg['name']
            self._log.debug('unregister raptor queue: %s', name)

            with self._raptor_lock:

                if name not in self._raptor_queues:
                    self._log.warn('raptor queue %s unknown [%s]', name, msg)

                else:
                    del(self._raptor_queues[name])

                if name in self._raptor_tasks:
                    tasks = self._raptor_tasks[name]
                    del(self._raptor_tasks[name])

                    self._log.debug('fail %d tasks: %d', len(tasks), name)
                    self.advance(tasks, state=rps.FAILED,
                                        publish=True, push=False)

        else:
            self._log.debug('command ignored: [%s]', cmd)

        return True


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

        # for node_name, node_id, cores, gpus in slots['ranks']:
        for rank in slots['ranks']:

            # Find the entry in the the slots list

            # TODO: [Optimization] Assuming 'node_id' is the ID of the node, it
            #       seems a bit wasteful to have to look at all of the nodes
            #       available for use if at most one node can have that uid.
            #       Maybe it would be worthwhile to simply keep a list of nodes
            #       that we would read, and keep a dictionary that maps the uid
            #       of the node to the location on the list?

            node = None
            node_found = False
            for node in self.nodes:
                if node['node_id'] == rank['node_id']:
                    node_found = True
                    break

            if not node_found:
                raise RuntimeError('inconsistent node information')

            # iterate over cores/gpus in the slot, and update state
            cores = rank['core_map']
            for cslot in cores:
                for core in cslot:
                    node['cores'][core] = new_state

            gpus = rank['gpu_map']
            for gslot in gpus:
                for gpu in gslot:
                    node['gpus'][gpu] = new_state

            if rank['lfs']:
                if new_state == rpc.BUSY:
                    node['lfs'] -= rank['lfs']
                else:
                    node['lfs'] += rank['lfs']

            if rank['mem']:
                if new_state == rpc.BUSY:
                    node['mem'] -= rank['mem']
                else:
                    node['mem'] += rank['mem']



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

        for uid, task in self._waitpool.items():
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

        #  FIXME: the component does not clean out subscribers after fork :-/
        self._subscribers = dict()

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

        #  subscribe to control messages, e.g., to register raptor queues
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

        # also keep a backlog of raptor tasks until their queues are registered
        self._raptor_queues = dict()           # raptor_master_id : zmq.Queue
        self._raptor_tasks  = dict()           # raptor_master_id : [task]
        self._raptor_lock   = mt.Lock()        # lock for the above

        # register task output channels
        self.register_output(rps.AGENT_EXECUTING_PENDING,
                             rpc.AGENT_EXECUTING_QUEUE)

        self._publishers = dict()
        self.register_publisher(rpc.STATE_PUBSUB)

        resources = True  # fresh start, all is free
        while not self._proc_term.is_set():

            self._log.debug_3('=== schedule tasks 0: %s, w: %d', resources,
                    len(self._waitpool))

            active = 0  # see if we do anything in this iteration

            # if we have new resources, try to place waiting tasks.
            r_wait = False
            if resources:
                r_wait, a = self._schedule_waitpool()
                active += int(a)
                self._log.debug_3('=== schedule tasks w: %s %s', r_wait, a)

            # always try to schedule newly incoming tasks
            # running out of resources for incoming could still mean we have
            # smaller slots for waiting tasks, so ignore `r` for now.
            r_inc, a = self._schedule_incoming()
            active += int(a)
            self._log.debug_3('=== schedule tasks i: %s %s', r_inc, a)

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
            self._log.debug_3('=== schedule tasks c: %s %s', r, a)

            if not active:
                time.sleep(0.1)  # FIXME: configurable

            self._log.debug_3('=== schedule tasks x: %s %s', resources, active)


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
        to_wait    = list()
        to_test    = list()

        for task in self._waitpool.values():
            named_env = task['description'].get('named_env')
            if named_env:
                if named_env in self._named_envs:
                    to_test.append(task)
                else:
                    to_wait.append(task)
            else:
                to_test.append(task)

        to_test.sort(key=lambda x:
                (x['tuple_size'][0] + x['tuple_size'][2]) * x['tuple_size'][1],
                 reverse=True)

        # cycle through waitpool, and see if we get anything placed now.
      # self._log.debug('=== before bisec: %d', len(to_test))
        scheduled, unscheduled, failed = ru.lazy_bisect(to_test,
                                                check=self._try_allocation,
                                                on_skip=self._prof_sched_skip,
                                                log=self._log)
      # self._log.debug('=== after  bisec: %d : %d : %d', len(scheduled),
      #                                           len(unscheduled), len(failed))

        for task, error in failed:
            task['stderr']       = error
            task['control']      = 'tmgr_pending'
            task['target_state'] = 'FAILED'
            task['$all']         = True

            self._log.error('bisect failed on %s: %s', task['uid'], error)
            self.advance(scheduled, rps.FAILED, publish=True, push=False)

        self._waitpool = {task['uid']: task for task in (unscheduled + to_wait)}

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
        to_schedule = list()  # some tasks get scheduled here
        to_raptor   = dict()  # some tasks get forwared to raptor
        try:

            while not self._proc_term.is_set():

                data = self._queue_sched.get(timeout=0.001)

                if not isinstance(data, list):
                    data = [data]

                for task in data:
                    # check if this task is to be scheduled by sub-schedulers
                    # like raptor
                    raptor = task['description'].get('scheduler')
                    if raptor:
                        if raptor not in to_raptor:
                            to_raptor[raptor] = [task]
                        else:
                            to_raptor[raptor].append(task)

                    else:
                        # no raptor - schedule it here
                        self._set_tuple_size(task)
                        to_schedule.append(task)

        except queue.Empty:
            # no more unschedule requests
            pass

        # forward raptor tasks to their designated raptor
        if to_raptor:

            with self._raptor_lock:

                for name in to_raptor:

                    if name in self._raptor_queues:
                        self._log.debug('fwd %s: %d', name,
                                        len(to_raptor[name]))
                        self._raptor_queues[name].put(to_raptor[name])

                    else:
                        self._log.debug('cache %s: %d', name,
                                        len(to_raptor[name]))

                        if name not in self._raptor_tasks:
                            self._raptor_tasks[name] = to_raptor[name]
                        else:
                            self._raptor_tasks[name] += to_raptor[name]

        if not to_schedule:
            # no resource change, no activity
            return None, False

      # self.slot_status("before schedule incoming [%d]" % len(to_schedule))

        # handle largest to_schedule first
        # FIXME: this needs lazy-bisect
        to_wait    = list()
        for task in sorted(to_schedule, key=lambda x: x['tuple_size'][0],
                           reverse=True):

            # FIXME: This is a slow and inefficient way to wait for named VEs.
            #        The semantics should move to the upcoming eligibility
            #        checker
            # FIXME: Note that this code is duplicated in _schedule_waitpool
            named_env = task['description'].get('named_env')
            if named_env:
                if named_env not in self._named_envs:
                    to_wait.append(task)
                    self._log.debug('delay %s, no env %s',
                                    task['uid'], named_env)
                    continue

            # either we can place the task straight away, or we have to
            # put it in the wait pool.
            try:
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

            except Exception as e:

                task['stderr']       = str(e)
                task['control']      = 'tmgr_pending'
                task['target_state'] = 'FAILED'
                task['$all']         = True

                self._log.exception('scheduling failed for %s', task['uid'])

                self.advance(task, rps.FAILED, publish=True, push=False)


        # all tasks which could not be scheduled are added to the waitpool
        self._waitpool.update({task['uid']: task for task in to_wait})

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
                task = self._queue_unsched.get(timeout=0.01)
                to_unschedule.append(task)
                if len(to_unschedule) > 512:
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

            self._active_cnt -= 1
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
        self._waitpool = {task['uid']: task for task in self._waitpool.values()
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
      # td  = task['description']

      # self._prof.prof('schedule_try', uid=uid)

        slots = self.schedule_task(task)
        if not slots:

            # schedule failure
          # self._prof.prof('schedule_fail', uid=uid)

            # if schedule fails while no other task is scheduled, then the
            # `schedule_task` will never be able to succeed - fail that task
            if self._active_cnt == 0:
                raise RuntimeError('insufficient resources')

            return False

        self._active_cnt += 1

        # the task was placed, we need to reflect the allocation in the
        # nodelist state (BUSY) and pass placement to the task, to have
        # it enacted by the executor
        self._change_slot_states(slots, rpc.BUSY)
        task['slots'] = slots

        # got an allocation, we can go off and launch the process
        self._prof.prof('schedule_ok', uid=uid)

        return True


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

