
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RPs's tmgr scheduler types
SCHEDULER_ROUND_ROBIN  = "round_robin"
SCHEDULER_BACKFILLING  = "backfilling"

# default:
SCHEDULER_DEFAULT      = SCHEDULER_ROUND_ROBIN

# internally used enums for pilot roles
ROLE    = '_scheduler_role'
ADDED   = 'added'
REMOVED = 'removed'
FAILED  = 'failed'


# ------------------------------------------------------------------------------
#
class TMGRSchedulingComponent(rpu.Component):

    # FIXME: clarify what can be overloaded by Scheduler classes

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.scheduling.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._tmgr = self._cfg.owner

        self._early        = dict()      # early-bound tasks, pid-sorted
        self._pilots       = dict()      # dict of known pilots
        self._pilots_lock  = ru.RLock()  # lock on the above dict
        self._tasks        = dict()      # dict of scheduled task IDs
        self._tasks_lock   = ru.RLock()  # lock on the above dict
        self._waiting      = dict()      # dict for tasks waiting on deps
        self._waiting_lock = dict()      # lock on the above dict

        # configure the scheduler instance
        self._configure()

        self.register_input(rps.TMGR_SCHEDULING_PENDING,
                            rpc.TMGR_SCHEDULING_QUEUE, self.work)

        self.register_output(rps.TMGR_STAGING_INPUT_PENDING,
                             rpc.TMGR_STAGING_INPUT_QUEUE)

        # Some schedulers care about states (of pilots and/or tasks), some
        # don't.  Either way, we here subscribe to state updates.
        self.register_subscriber(rpc.STATE_PUBSUB, self._base_state_cb)

        # Schedulers use that command channel to get information about
        # pilots being added or removed.
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._base_command_cb)

        # cache the local client sandbox to avoid repeated os calls
        self._client_sandbox = os.getcwd()


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != TMGRSchedulingComponent:
            raise TypeError("Scheduler Factory only available to base class!")

        name = cfg['scheduler']

        from .round_robin  import RoundRobin
        from .backfilling  import Backfilling

        try:
            impl = {
                SCHEDULER_ROUND_ROBIN : RoundRobin,
                SCHEDULER_BACKFILLING : Backfilling
            }[name]

            impl = impl(cfg, session)
            return impl

        except KeyError as e:
            raise ValueError("Scheduler '%s' unknown or defunct" % name) from e


    # --------------------------------------------------------------------------
    #
    def _base_state_cb(self, topic, msg):

        # the base class will keep track of pilot state changes and updates
        # self._pilots accordingly.  Task state changes will also be collected,
        # but not stored - if a scheduler needs to keep track of task state
        # changes, it needs to overload `update_tasks()`.

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        self._log.info('scheduler state_cb: %s', cmd)

        # FIXME: get cmd string consistent throughout the code
        if cmd not in ['update', 'state_update']:
            self._log.debug('ignore cmd %s', cmd)
            return True

        if not isinstance(arg, list): things = [arg]
        else                        : things =  arg

        pilots = [t for t in things if t['type'] == 'pilot']
        tasks  = [t for t in things if t['type'] == 'task' ]

        self._log.debug('update pilots %s', [p['uid'] for p in pilots])
        self._log.debug('update tasks  %s', [u['uid'] for u in tasks])

        self._update_pilot_states(pilots)
        self._update_task_states(tasks)

        return True


    # --------------------------------------------------------------------------
    #
    def _update_pilot_states(self, pilots):

        self._log.debug('update pilot states for %s', [p['uid'] for p in pilots])

        if not pilots:
            return

        to_update = list()

        with self._pilots_lock:

            for pilot in pilots:

                pid = pilot['uid']

                if pid not in self._pilots:
                    self._pilots[pid] = {'role'  : None,
                                         'state' : None,
                                         'pilot' : None,
                                         'info'  : dict()  # scheduler private info
                                         }

                target  = pilot['state']
                current = self._pilots[pid]['state']

                # enforce state model order
                target, passed = rps._pilot_state_progress(pid, current, target)

                if current != target:
                    to_update.append(pid)
                    self._pilots[pid]['state'] = target
                    self._log.debug('update pilot state: %s -> %s', current, passed)

        if to_update:
            self.update_pilots(to_update)
        self._log.debug('updated  : %s', to_update)


    # --------------------------------------------------------------------------
    #
    def _update_task_states(self, tasks):

        self.update_tasks(tasks)


    # --------------------------------------------------------------------------
    #
    def update_tasks(self, uids):
        '''
        any scheduler that cares about task state changes should implement this
        method to keep track of those
        '''

        pass


    # --------------------------------------------------------------------------
    #
    def _base_command_cb(self, topic, msg):

        # we'll wait for commands from the tmgr, to learn about pilots we can
        # use or we should stop using. We also track task cancelation, as all
        # components do.
        #
        # make sure command is for *this* scheduler by matching the tmgr uid.

        cmd = msg['cmd']

        self._log.debug('got cmd %s', cmd)

        if cmd not in ['add_pilots', 'remove_pilots', 'cancel_tasks']:
            return True

        arg   = msg['arg']
        tmgr  = arg['tmgr']

        self._log.info('scheduler command: %s: %s' % (cmd, arg))

        if tmgr and tmgr != self._tmgr:
            # this is not the command we are looking for
            return True


        if cmd == 'add_pilots':

            pilots = arg['pilots']

            with self._pilots_lock:

                for pilot in pilots:

                    pid = pilot['uid']

                    if pid in self._pilots:
                        if self._pilots[pid]['role'] == ADDED:
                            raise ValueError('pilot already added (%s)' % pid)
                    else:
                        self._pilots[pid] = {'role'  : None,
                                             'state' : None,
                                             'pilot' : None,
                                             'info'  : dict()
                                            }

                    self._pilots[pid]['role']  = ADDED
                    self._pilots[pid]['pilot'] = pilot
                    self._log.debug('added pilot: %s', self._pilots[pid])

                self._update_pilot_states(pilots)

                for pilot in pilots:

                    pid = pilot['uid']

                    # if we have any early_bound tasks waiting for this pilots,
                    # advance them now
                    early_tasks = self._early.get(pid)
                    if early_tasks:

                        for task in early_tasks:
                            self._assign_pilot(task, pilot)

                        self.advance(early_tasks, rps.TMGR_STAGING_INPUT_PENDING,
                                     publish=True, push=True)

            # let the scheduler know
            self.add_pilots([pilot['uid'] for pilot in pilots])


        elif cmd == 'remove_pilots':

            pids = arg['pids']

            with self._pilots_lock:

                for pid in pids:

                    if pid not in self._pilots:
                        raise ValueError('pilot not added (%s)' % pid)

                    if self._pilots[pid]['role'] != ADDED:
                        raise ValueError('pilot not added (%s)' % pid)

                    self._pilots[pid]['role'] = REMOVED
                    self._log.debug('removed pilot: %s', self._pilots[pid])

            # let the scheduler know
            self.remove_pilots(pids)


        elif cmd == 'cancel_tasks':

            uids = arg['uids']

            # find the pilots handling these tasks and forward the cancellation
            # request
            to_cancel = dict()

            with self._tasks_lock:
                for pid in self._tasks:
                    for uid in uids:
                        if uid in self._tasks[pid]:
                            if pid not in to_cancel:
                                to_cancel[pid] = list()
                            to_cancel[pid].append(uid)

            dbs = self._session._dbs

            if not dbs:
                # too late, already closing down
                return True

            for pid in to_cancel:
                dbs.pilot_command(cmd='cancel_tasks',
                                  arg={'uids' : to_cancel[pid]},
                                  pids=pid)

        return True


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        raise NotImplementedError("_configure() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def _assign_pilot(self, task, pilot):
        '''
        assign a task to a pilot.
        This is also a good opportunity to determine the task sandbox(es).
        '''

        pid = pilot['uid']
        uid = task['uid']

        self._log.debug('assign %s to %s', uid, pid)

        task['pilot'           ] = pid
        task['client_sandbox'  ] = str(self._session._get_client_sandbox())
        task['resource_sandbox'] = str(self._session._get_resource_sandbox(pilot))
        task['pilot_sandbox'   ] = str(self._session._get_pilot_sandbox(pilot))
        task['task_sandbox'    ] = str(self._session._get_task_sandbox(task, pilot))

        task['task_sandbox_path'] = ru.Url(task['task_sandbox']).path

        with self._tasks_lock:
            if pid not in self._tasks:
                self._tasks[pid] = list()
            self._tasks[pid].append(uid)


    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pids):
        raise NotImplementedError("add_pilots() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pids):
        raise NotImplementedError("remove_pilots() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def update_pilots(self, pids):
        raise NotImplementedError("update_pilots() missing for '%s'" % self.uid)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):
        '''
        We get a number of tasks, and filter out those which are already bound
        to a pilot.  Those will get advanced to TMGR_STAGING_INPUT_PENDING
        straight away.  All other tasks are passed on to `self._work()`, which
        is the scheduling routine as implemented by the deriving scheduler
        classes.

        Note that the filter needs to know any pilot the tasks are early-bound
        to, as it needs to obtain the task's sandbox information to prepare for
        the follow-up staging ops.  If the respective pilot is not yet known,
        the tasks are put into a early-bound-wait list, which is checked and
        emptied as pilots get registered.
        '''

        if not isinstance(tasks, list):
            tasks = [tasks]

        # some task may have staging directives which reference sandboxes of
        # other tasks.  Those directives can only be expanded with actual
        # physical path's once both tasks are known to the scheduler, so we
        # check this here and (a) let all tasks wait until the references are
        # resolved, and (b) check if the tasks resolve any references
        #
        # The `waiting` data structure has the following format:
        #
        #   {
        #     'waiting': {
        #         <uid_a> : {
        #             'task': <task>,
        #             'deps': [<uid_1>, <uid_2>, ...]
        #         },
        #         ...
        #       },
        #     'deps': {
        #       <uid_1> : [<uid_a>, <uid_b>, ...]
        #       ...
        #   }
        #
        # for each incoming tasks <uid_1>, we check in `deps` if depending tasks
        # are known, then remove <uid_1> from the global `deps` dict and also
        # from the `deps` list of each of those waiting tasks.  If any of those
        # waiting tasks then ends up with an empty `deps` list, then that task
        # will not be waiting anymore and can be scheduled.  We mark both
        # participating tasks so that the scheduler can ensure they end up on
        # the same pilot.
        #
        # NOTE: cross-pilot data dependencies are not yet supported
        #
        # The task staging directives are expected to be expanded to their
        # dictionary format already, and will check for `src` or `tgt` URLs with
        # a `sandbox://` schema, where the `host` element can reference
        #
        #   client:   the client application pwd
        #   resource: the target resource sandbox
        #   pilot:    the target pilot's sandbox
        #   <uid>:    the sandbox of the respective task
        #
        # This implies that `client`, `resource` and `pilot` are reserved names
        # for task IDs, and that tasks which use invalid / non-existing IDs in
        # sandbox references will never be eligible for scheduling.

        self.advance(tasks, rps.TMGR_SCHEDULING, publish=True, push=False)

        to_schedule = list()

        with self._pilots_lock:

            for task in tasks:

                uid = task['uid']
                pid = task.get('pilot')

                if pid:
                    # this task is bound already (it is early-bound), so we
                    # don't need to pass it to the actual scheduling algorithm

                    # check if we know about the pilot, so that we can advance
                    # the task to data staging
                    pilot = self._pilots.get(pid, {}).get('pilot')
                    if pilot:
                        self._assign_pilot(task, pilot)
                        self.advance(task, rps.TMGR_STAGING_INPUT_PENDING,
                                     publish=True, push=True)

                    else:
                        # otherwise keep in `self._early` until we learn about
                        # the pilot
                        self._log.warn('got task %s for unknown pilot %s', uid, pid)
                        if pid not in self._early:
                            self._early[pid] = list()
                        self._early[pid].append(task)

                else:
                    to_schedule.append(task)

        self._log.debug('to_schedule: %d', len(to_schedule))
        self._work(to_schedule)


    # --------------------------------------------------------------------------
    #
    def _work(self, tasks):

        raise NotImplementedError("work() missing for '%s'" % self.uid)


# ------------------------------------------------------------------------------

