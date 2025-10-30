
__copyright__ = 'Copyright 2013-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import copy
import time
import queue

from collections import defaultdict

import threading          as mt
import multiprocessing    as mp

import radical.utils      as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from ...task_description import RAPTOR_WORKER
from ..resource_manager  import ResourceManager


# ------------------------------------------------------------------------------
#
# 'enum' for RPs's pilot scheduler types
#
SCHEDULER_NAME_CONTINUOUS_ORDERED  = "CONTINUOUS_ORDERED"
SCHEDULER_NAME_CONTINUOUS_RECONFIG = "CONTINUOUS_RECONFIG"
SCHEDULER_NAME_CONTINUOUS_COLO     = "CONTINUOUS_COLO"
SCHEDULER_NAME_CONTINUOUS_JSRUN    = "CONTINUOUS_JSRUN"
SCHEDULER_NAME_CONTINUOUS          = "CONTINUOUS"
SCHEDULER_NAME_HOMBRE              = "HOMBRE"
SCHEDULER_NAME_FLUX                = "FLUX"
SCHEDULER_NAME_NOOP                = "NOOP"

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
#       'ranks'         : 4,
#       'cores_per_rank': 2,
#       'gpus_per_rank  : 2.,
#       'slots'         :
#                       # [[node,   node_index, [cpu map], [gpu map]]]
#                         [[node_1, 0,          [0, 2],    [0],
#                          [node_1, 0,          [4, 6],    [0],
#                          [node_2, 1,          [5, 7],    [0],
#                          [node_2, 1,          [1, 3]],   [0]],
#
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
# (`cores_per_rank=2`).
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
class AgentSchedulingComponent(rpu.AgentComponent):

    # flags for the scheduler queue
    _SCHEDULE = 1
    _CANCEL   = 2

    # --------------------------------------------------------------------------
    #
    # the deriving schedulers should in general have the following structure in
    # self.nodes:
    #
    #   self.nodes = [
    #     { 'name'  : 'name-of-node',
    #       'index' : 'idx-of-node',
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

        self.nodes = []
        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize()` will be called
    # before control is given to the component's main loop.
    #
    def initialize(self):

        # The scheduler needs the ResourceManager information which have been
        # collected during agent startup.
        rm_name  = self.session.rcfg.resource_manager
        self._rm = ResourceManager.create(rm_name,
                                          self.session.cfg,
                                          self.session.rcfg,
                                          self._log, self._prof)

        self._partition_ids = self._rm.get_partition_ids()

        # create and initialize the wait pool.  Also maintain a mapping of that
        # waitlist to a binned list where tasks are binned by size for faster
        # lookups of replacement tasks.  And outdated binlist is mostly
        # sufficient, only rebuild when we run dry
        #
        # NOTE: the waitpool is really a dict of dicts where the first level key
        #       is the task priority.
        #
        self._waitpool   = defaultdict(dict)  # {priority : {uid:task}}
        self._ts_map     = defaultdict(set)   # tasks binned by tuple size
        self._ts_valid   = False              # set to False to trigger re-binning
        self._active_cnt = 0                  # count of currently scheduled tasks
        self._named_envs = list()             # record available named environments

        # the scheduler algorithms have two inputs: tasks to be scheduled, and
        # slots becoming available (after tasks complete).
        self._queue_sched   = mp.Queue()
        self._queue_unsched = mp.Queue()
        self._term          = mp.Event()  # reassign Event (multiprocessing)

        # initialize the node list to be used by the scheduler.  A scheduler
        # instance may decide to overwrite or extend this structure.
        self.nodes = copy.deepcopy(self._rm.info.node_list)

        # configure the scheduler instance
        self._configure()
        self.slot_status("after  init")

        # register task input channels
        self.register_input(rps.AGENT_SCHEDULING_PENDING,
                            rpc.AGENT_SCHEDULING_QUEUE, self.work)

        # we need unschedule updates to learn about tasks for which to free the
        # allocated cores.  Those updates MUST be issued after execution, ie.
        # by the AgentExecutionComponent.
        self.register_subscriber(rpc.AGENT_UNSCHEDULE_PUBSUB, self.unschedule_cb)

        # start a process to host the actual scheduling algorithm
        self._scheduler_process = False
        self._p = mp.Process(target=self._schedule_tasks)
        self._p.daemon = True
        self._p.start()

        self._pwatcher = ru.PWatcher(uid='%s.pw' % self._uid, log=self._log)
        self._pwatcher.watch(os.getpid())
        self._pwatcher.watch(self._p.pid)


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

        name = session.rcfg.agent_scheduler

        from .continuous_ordered  import ContinuousOrdered
        from .continuous_reconfig import ContinuousReconfig
        from .continuous_colo     import ContinuousColo
        from .continuous_jsrun    import ContinuousJsrun
        from .continuous          import Continuous
        from .hombre              import Hombre
        from .flux                import Flux
        from .noop                import Noop

        # if `jsrun` is used as lauunch method, then we switch the CONTINUOUS
        # scheduler with CONTINUOUS_JSRUN
        if 'JSRUN' in session.rcfg.launch_methods:
            if name == SCHEDULER_NAME_CONTINUOUS:
                name = SCHEDULER_NAME_CONTINUOUS_JSRUN

        impl = {

            SCHEDULER_NAME_CONTINUOUS_ORDERED  : ContinuousOrdered,
            SCHEDULER_NAME_CONTINUOUS_RECONFIG : ContinuousReconfig,
            SCHEDULER_NAME_CONTINUOUS_COLO     : ContinuousColo,
            SCHEDULER_NAME_CONTINUOUS_JSRUN    : ContinuousJsrun,
            SCHEDULER_NAME_CONTINUOUS          : Continuous,
            SCHEDULER_NAME_HOMBRE              : Hombre,
            SCHEDULER_NAME_FLUX                : Flux,
            SCHEDULER_NAME_NOOP                : Noop,
        }

        if name not in impl:
            raise ValueError('Scheduler %s unknown' % name)

        return impl[name](cfg, session)


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):
        '''
        listen on the control channel for raptor queue registration commands
        '''

        # only the scheduler process listens for control messages
        if not self._scheduler_process:
            return

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'register_named_env':

            env_name = arg['env_name']
            self._named_envs.append(env_name)


        elif cmd == 'register_raptor_queue':

            name  = arg['name']
            queue = arg['queue']
            addr  = arg['addr']

            self._log.debug('register raptor queue: %s', name)
            with self._raptor_lock:

                self._raptor_queues[name] = ru.zmq.Putter(queue, addr)

                # send tasks which were collected for this queue
                if name in self._raptor_tasks:

                    tasks = self._raptor_tasks[name]
                    del self._raptor_tasks[name]

                    self._log.debug('relay %d tasks to raptor %s', len(tasks), name)
                    self._raptor_queues[name].put(tasks)

                # also send any tasks which were collected for *any* queue
                if '*' in self._raptor_tasks:

                    tasks = self._raptor_tasks['*']
                    del self._raptor_tasks['*']

                    self._log.debug('* relay %d tasks to raptor %s', len(tasks), name)
                    self._raptor_queues[name].put(tasks)


        elif cmd == 'unregister_raptor_queue':

            name = arg['name']
            self._log.debug('unregister raptor queue: %s', name)

            with self._raptor_lock:

                if name not in self._raptor_queues:
                    self._log.warn('raptor queue %s unknown [%s]', name, msg)

                else:
                    del self._raptor_queues[name]

                if name in self._raptor_tasks:
                    tasks = self._raptor_tasks[name]
                    del self._raptor_tasks[name]

                    self._log.debug('fail %d tasks: %d', len(tasks), name)

                    for task in tasks:
                        self._fail_task(task, RuntimeError('raptor gone'),
                                              'raptor queue disappeared')


        elif cmd == 'cancel_tasks':

            self._log.debug('cancel tasks: %s', arg)

          # for priority in self._waitpool:
          #     self._log.debug('waitpool[%d]: %s', priority,
          #                     list(self._waitpool[priority].keys()))

            # inform the scheduler process
            uids = arg['uids']
            self._queue_sched.put((uids, self._CANCEL))

            # also cancel any raptor tasks we know about
            to_cancel = list()
            with self._raptor_lock:
                for queue in self._raptor_tasks:
                    matches = [t for t in self._raptor_tasks[queue]
                                       if t['uid'] in uids]
                    for task in matches:
                        to_cancel.append(task)
                        self._raptor_tasks[queue].remove(task)

            self.advance(to_cancel, rps.CANCELED, push=True, publish=True)

        else:
            self._log.debug('command ignored: [%s]', cmd)


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
        # FIXME: remove once Slot structure settles
        slots = rpu.convert_slots_to_new(slots)

        # for node_name, node_index, cores, gpus in slots['ranks']:
        for slot in slots:

            # Find the entry in the slots list

            # TODO: [Optimization] Assuming 'node_index' is the ID of the node,
            #       it seems a bit wasteful to have to look at all of the nodes
            #       available for use if at most one node can have that uid.
            #       Maybe it would be worthwhile to simply keep a list of nodes
            #       that we would read, and keep a dictionary that maps the uid
            #       of the node to the location on the list?

            node = None
            node_found = False
            for node in self.nodes:
                if node['index'] == slot['node_index']:
                    node_found = True
                    break

            if not node_found:
                raise RuntimeError('inconsistent node information')

            # iterate over cores/gpus in the slot, and update state
            for core in slot['cores']:
                node['cores'][core['index']] = new_state

            for gpu in slot['gpus']:
                node['gpus'][gpu['index']] = new_state

            if slot['lfs']:
                if new_state == rpc.BUSY:
                    node['lfs'] -= slot['lfs']
                else:
                    node['lfs'] += slot['lfs']

            if slot['mem']:
                if new_state == rpc.BUSY:
                    node['mem'] -= slot['mem']
                else:
                    node['mem'] += slot['mem']



    # --------------------------------------------------------------------------
    #
    # NOTE: any scheduler implementation which uses a different nodelist
    #       structure MUST overload this method.
    def slot_status(self, msg=None, uid=None):
        '''
        Returns a multi-line string corresponding to the status of the node list
        '''

        # need to set `DEBUG_5` or higher to get slot debug logs
        if self._log._debug_level < 5:
            return

        if not msg: msg = ''

        glyphs = {rpc.FREE : '-',
                  rpc.BUSY : '#',
                  rpc.DOWN : '!',
                  2        : '!'}  # FIXME: backward compatible, old slots
        ret = "|"
        for node in self.nodes:
            for core in node['cores']:
                ret += glyphs[core]
            ret += ':'
            for gpu in node['gpus']:
                ret += glyphs[gpu]
            ret += '|'

        if not uid:
            uid = ''

        self._log.debug("status: %-30s [%25s]: %s", msg, uid, ret)

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
        for priority in self._waitpool:

            if not self._waitpool[priority]:
                continue

            for uid, task in self._waitpool[priority].items():
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

        This method takes care of initial state change to `AGENT_SCHEDULING`,
        and then puts them forward onto the queue towards the actual scheduling
        process (self._schedule_tasks).
        '''

        # advance state, publish state change, and push to scheduler process
        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)
        self._queue_sched.put((tasks, self._SCHEDULE))


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

        self._scheduler_process = True

        self._pwatcher = ru.PWatcher(uid='%s.pw' % self._uid, log=self._log)
        self._pwatcher.watch(os.getpid())
        self._pwatcher.watch(os.getppid())

        # ZMQ endpoints will not have survived the fork. Specifically the
        # registry client of the component base class will have to reconnect.
        # Note that `self._reg` of the base class is a *pointer* to the sesison
        # registry.
        #
        # FIXME: should be moved into a post-fork hook of the session
        #
        self._reg = ru.zmq.RegistryClient(url=self.session.cfg.reg_addr)

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

        # keep a backlog of raptor tasks until their queues are registered
        self._raptor_queues = dict()           # raptor_master_id : zmq.Queue
        self._raptor_tasks  = dict()           # raptor_master_id : [task]
        self._raptor_lock   = mt.Lock()        # lock for the above

        # register task output channels
        self.register_output(rps.AGENT_EXECUTING_PENDING,
                             rpc.AGENT_EXECUTING_QUEUE)

        # re-register the control callback in this subprocess
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

        self._publishers = dict()
        self.register_publisher(rpc.STATE_PUBSUB)

        resources = True  # fresh start, all is free
        while not self._term.is_set():

            self._log.debug_3('schedule tasks 0: %s, w: %d', resources,
                    sum([len(pool) for pool in self._waitpool.values()]))

            active = 0  # see if we do anything in this iteration

            # if we have new resources, try to place waiting tasks.
            r_wait = False
            if resources:
                r_wait, a = self._schedule_waitpool()
                active += int(a)
                self._log.debug_3('schedule tasks w: %s %s', r_wait, a)

            # always try to schedule newly incoming tasks
            # running out of resources for incoming could still mean we have
            # smaller slots for waiting tasks, so ignore `r` for now.
            r_inc, a = self._schedule_incoming()
            active += int(a)
            self._log.debug_3('schedule tasks i: %s %s', r_inc, a)

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
            self._log.debug_3('schedule tasks c: %s %s', r, a)

            if not active:
                time.sleep(0.1)  # FIXME: configurable

            self._log.debug_3('schedule tasks x: %s %s', resources, active)


    # --------------------------------------------------------------------------
    #
    def _prof_sched_skip(self, task):

      # self._prof.prof('schedule_skip', uid=task['uid'])
        pass


    # --------------------------------------------------------------------------
    #
    def _schedule_waitpool(self):

      # self.slot_status("before schedule waitpool")

        # sort by inverse tuple size to place larger tasks first and backfill
        # with smaller tasks.  We only look at cores right now - this needs
        # fixing for GPU dominated loads.
        # We define `tuple_size` as
        #     `ranks * cores_per_rank * gpus_per_rank`
        #
        active    = False  # nothing happeend yet
        resources = True   # fresh start, all is free

        for priority in sorted(self._waitpool.keys(), reverse=True):

            to_wait   = list()
            to_test   = list()

            pool = self._waitpool[priority]
            if not pool:
                continue

            self._log.debug_5('schedule waitpool[%d]: %d', priority, len(pool))

            for task in pool.values():
                named_env = task['description'].get('named_env')
                if named_env:
                    if named_env in self._named_envs:
                        to_test.append(task)
                    else:
                        to_wait.append(task)
                else:
                    to_test.append(task)

            to_test.sort(key=lambda x:
                    x['tuple_size'][0] * x['tuple_size'][1] * x['tuple_size'][2],
                     reverse=True)

            if not to_test:
                # all tasks went into to_wait, so there is nothing to do
                self._log.debug_8('no tasks to bisect')
                continue

            # cycle through waitpool, and see if we get anything placed now.
            self._log.debug_9('before bisect: %d', len(to_test))
            scheduled, unscheduled, failed = ru.lazy_bisect(to_test,
                                                    check=self._try_allocation,
                                                    on_skip=self._prof_sched_skip,
                                                    log=self._log)
            self._log.debug_9('after  bisect: %d : %d : %d', len(scheduled),
                                                      len(unscheduled), len(failed))

            for task, error in failed:
                error  = error.replace('"', '\\"')
                self._fail_task(task, RuntimeError('bisect failed'), error)
                self._log.error('bisect failed on %s: %s', task['uid'], error)

            self._waitpool[priority] = {task['uid']: task
                                            for task in (unscheduled + to_wait)}

            # update task resources
            for task in scheduled:
                td = task['description']
                task['$set']      = ['resources']
                task['resources'] = {'cpu': td['ranks'] * td['cores_per_rank'],
                                     'gpu': td['ranks'] * td['gpus_per_rank']}
            self.advance(scheduled, rps.AGENT_EXECUTING_PENDING, publish=True,
                                                                 push=True)

            # method counts as `active` if anything was scheduled
            active = active or bool(scheduled)

            # if we sccheduled some tasks but not all, we ran out of resources
            resources = resources and not (bool(unscheduled))

      # self.slot_status("after  schedule waitpool")
        return resources, active


    # --------------------------------------------------------------------------
    #
    def _fail_task(self, task, e, detail):

        task['exception']        = repr(e)
        task['exception_detail'] = detail

        self._log.exception('scheduling failed for %s', task['uid'])

        self.advance(task, rps.FAILED, publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def _schedule_incoming(self):

        # fetch all tasks from the queue
        to_schedule = defaultdict(list)  # some tasks get scheduled here
        to_raptor   = defaultdict(list)  # some tasks get forwared to raptor
        try:

            while not self._term.is_set():

                data, flag = self._queue_sched.get(timeout=0.001)

                assert flag in [self._SCHEDULE, self._CANCEL], flag

                if flag == self._CANCEL:
                    to_cancel = list()
                    for uid in data:
                        for priority in self._waitpool:
                            task = self._waitpool[priority].get(uid)
                            if task:
                                to_cancel.append(task)
                                del self._waitpool[priority][uid]
                                break

                    self.advance(to_cancel, rps.CANCELED,
                                                       push=False, publish=True)

                    # nothing to schedule, pull from queue again
                    continue

                # we got a task to schedule
                for task in data:

                    td = task['description']

                    if td.get('ranks') <= 0:
                        self._fail_task(task, ValueError('invalid ranks'), '')

                    # check if this task is to be scheduled by sub-schedulers
                    # like raptor
                    raptor_id = td.get('raptor_id')
                    priority  = td.get('priority', 0)
                    mode      = td.get('mode')

                    self._log.debug('incoming task %s (%s)', task['uid'], priority)

                    # raptor workers are not scheduled by raptor itself!
                    if raptor_id and mode != RAPTOR_WORKER:

                        if task.get('raptor_seen'):
                            # raptor has handled this one - we can execute it
                            self._set_tuple_size(task)
                            to_schedule[priority].append(task)

                        else:
                            to_raptor[raptor_id].append(task)

                    else:
                        # no raptor - schedule it here
                        self._set_tuple_size(task)
                        to_schedule[priority].append(task)

        except queue.Empty:
            # no more schedule requests
            pass

        # forward raptor tasks to their designated raptor
        if to_raptor:

            with self._raptor_lock:

                for name in to_raptor:

                    if name in self._raptor_queues:
                        # forward to specified raptor queue
                        self._log.debug('fwd %s: %d', name,
                                        len(to_raptor[name]))
                        self._raptor_queues[name].put(to_raptor[name])

                    elif self._raptor_queues and name == '*':
                        # round robin to available raptor queues
                        names   = list(self._raptor_queues.keys())
                        n_names = len(names)
                        for idx in range(len(to_raptor[name])):
                            task  = to_raptor[name][idx]
                            qname = names[idx % n_names]
                            self._log.debug('* put task %s to rq %s',
                                    task['uid'], qname)
                            self._raptor_queues[qname].put(task)

                    else:
                        # keep around until a raptor queue registers
                        self._log.debug('cache %s: %d', name,
                                        len(to_raptor[name]))

                        if name not in self._raptor_tasks:
                            self._raptor_tasks[name] = to_raptor[name]
                        else:
                            self._raptor_tasks[name] += to_raptor[name]

        if not to_schedule:
            # no resource change, no activity
            return None, False

        n_tasks = sum([len(x) for x in to_schedule.values()])
      # self.slot_status("before schedule incoming [%d]" % n_tasks)

        # handle largest to_schedule first
        # FIXME: this needs lazy-bisect
        for priority in sorted(to_schedule.keys(), reverse=True):

            tasks   = to_schedule[priority]
            to_wait = list()
            for task in sorted(tasks, key=lambda x: x['tuple_size'][0],
                               reverse=True):

                td = task['description']

                # FIXME: This is a slow, inefficient way to wait for named VEs.
                #        The semantics should move to the upcoming eligibility
                #        checker
                # FIXME: Note that this code is duplicated in _schedule_waitpool
                named_env = td.get('named_env')
                if named_env:
                    if named_env not in self._named_envs:
                        to_wait.append(task)
                        self._log.debug('delay %s, no env %s',
                                        task['uid'], named_env)
                        continue

                # we actually only schedule tasks which do not yet have
                # a `slots` structure attached.  Those which do were presumably
                # scheduled earlier (by the applicaiton of some other client
                # side scheduler), and we honor that decision.  We though will
                # mark the respective resources as being used, to avoid other
                # tasks being scheduled onto the same set of resources.
                if td.get('slots'):

                    task['slots']     = td['slots']
                    task['partition'] = td['partition']
                    task['resources'] = {'cpu': td['ranks'] * td['cores_per_rank'],
                                         'gpu': td['ranks'] * td['gpus_per_rank']}
                    self.advance(task, rps.AGENT_EXECUTING_PENDING,
                                 publish=True, push=True, fwd=True)
                    continue


                # either we can place the task straight away, or we have to
                # put it in the wait pool.
                try:
                    if self._try_allocation(task):
                        # task got scheduled - advance state, notify world about the
                        # state change, and push it out toward the next component.
                        td = task['description']
                        task['$set']      = ['resources']
                        task['resources'] = {'cpu': td['ranks'] *
                                                    td['cores_per_rank'],
                                             'gpu': td['ranks'] *
                                                    td['gpus_per_rank']}
                        self.advance(task, rps.AGENT_EXECUTING_PENDING,
                                     publish=True, push=True, fwd=True)

                    else:
                        to_wait.append(task)

                except Exception as e:
                    self._fail_task(task, e, '\n'.join(ru.get_exception_trace()))


            # all tasks which could not be scheduled are re-added to the waitpool
            for task in to_wait:
                uid = task['uid']
                self._waitpool[priority][uid] = task

                # now that we added the task to the waitpool, check if a cancel
                # request has meanwhile arrived - if so remove it, otherwise it
                # will get removed during the next iteration of the main loop
                if self.is_canceled(task) is True:
                    del self._waitpool[priority][uid]

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
            # sleep, avoid no sleep (busy idle), for the bulk size, avoid
            # waiting for too long to fill a bulk so that latencies add a up
            # and negate the bulk optimization.
            while not self._term.is_set():
                tasks = self._queue_unsched.get(timeout=0.01)
                to_unschedule += ru.as_list(tasks)
                if len(to_unschedule) > 512:
                    break

        except queue.Empty:
            # no more unschedule requests
            pass

        to_release = list()  # slots of unscheduling tasks
      # placed     = list()  # uids of waiting tasks replacing unscheduled ones

        if to_unschedule:

            # rebuild the tuple_size binning, maybe
            self._refresh_ts_map()


        for task in to_unschedule:
      #     # if we find a waiting task with the same tuple size, we don't free
      #     # the slots, but just pass them on unchanged to the waiting task.
      #     # Thus we replace the unscheduled task on the same cores / GPUs
      #     # immediately. This assumes that the `tuple_size` is good enough to
      #     # judge the legality of the resources for the new target task.
      #     #
      #     # FIXME: use tuple size again
      #     # FIXME: consider priorities
      #     ts = tuple(task['tuple_size'])
      #     if self._ts_map[ts]:
      #
      #         prio  = ts[-1]
      #         r_uid = self._ts_map[ts].pop()
      #
      #         replace = None
      #         for prio in self._waitpool:
      #             replace = self._waitpool[prio].get(r_uid)
      #             if replace:
      #                 del self._waitpool[prio][r_uid]
      #                 break
      #
      #         if not replace:
      #             to_release.append(task)
      #
      #         else:
      #             replace['slots'] = task['slots']
      #             placed.append(placed)
      #
      #             # unschedule task A and schedule task B have the same
      #             # timestamp
      #             ts = time.time()
      #             self._prof.prof('unschedule_stop', uid=task['uid'], ts=ts)
      #             self._prof.prof('schedule_ok', uid=r_uid, ts=ts)
      #             self._prof.prof('schedule_fast', uid=r_uid, ts=ts)
      #             self.advance(replace, rps.AGENT_EXECUTING_PENDING,
      #                          publish=True, push=True)
      #     else:
      #
      #         # no replacement task found: free the slots, and try to
      #         # schedule other tasks of other sizes.
      #         to_release.append(task)

            to_release.append(task)
            self._active_cnt -= 1

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
          # self.slot_status("before unschedule", task['uid'])
            self.unschedule_task(task)
          # self.slot_status("after  unschedule", task['uid'])
            self._prof.prof('unschedule_stop', uid=task['uid'])

      # # we placed some previously waiting tasks, and need to remove those from
      # # the waitpool
      # # FIXME: see FIXMEs above
      # for priority in self._waitpool:
      #     tasks = self._waitpool[priority].values()
      #     self._waitpool[priority] = {task['uid']: task
      #                                     for task in tasks
      #                                     if  task['uid'] not in placed}

        # we have new resources, and were active
        return True, True


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, task):
        '''
        attempt to allocate cores/gpus for a specific task.
        '''

        try:
            uid = task['uid']
          # td  = task['description']

          # self._prof.prof('schedule_try', uid=uid)
            slots, partition = self.schedule_task(task)
            if not slots:

                # schedule failure
              # self._prof.prof('schedule_fail', uid=uid)

                # if schedule fails while no other task is scheduled, then the
                # `schedule_task` will never be able to succeed - fail that task
                if self._active_cnt == 0:
                    raise RuntimeError('task can never be scheduled')

                return False

            self._active_cnt += 1

            # the task was placed, we need to reflect the allocation in the
            # nodelist state (BUSY) and pass placement to the task, to have
            # it enacted by the executor
            self._change_slot_states(slots, rpc.BUSY)
            task['slots']     = slots
            task['partition'] = partition

            self.slot_status('after scheduled task', task['uid'])

            # got an allocation, we can go off and launch the process
            self._prof.prof('schedule_ok', uid=uid)

        except Exception as e:
            task['exception']        = repr(e)
            task['exception_detail'] = '\n'.join(ru.get_exception_trace())
            raise

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
        task['tuple_size'] = tuple([d.get('ranks'         , 1),
                                    d.get('cores_per_rank', 1),
                                    d.get('gpus_per_rank' , 0.)])


# ------------------------------------------------------------------------------

