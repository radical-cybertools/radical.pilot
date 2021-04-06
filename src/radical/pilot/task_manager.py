
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import time
import threading as mt

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc

from . import task_description as rpcud


# bulk callbacks are implemented, but are currently not used nor exposed.
_USE_BULK_CB = False
if os.environ.get('RADICAL_PILOT_BULK_CB', '').lower() in ['true', 'yes', '1']:
    _USE_BULK_CB = True


# ------------------------------------------------------------------------------
#
# make sure deprecation warning is shown only once per type
#
_seen = list()


def _warn(old_type, new_type):
    if old_type not in _seen:
        _seen.append(old_type)
        sys.stderr.write('%s is deprecated - use %s\n' % (old_type, new_type))


# ------------------------------------------------------------------------------
#
class TaskManager(rpu.Component):
    """
    A TaskManager manages :class:`radical.pilot.Task` instances which
    represent the **executable** workload in RADICAL-Pilot. A TaskManager
    connects the Tasks with one or more :class:`Pilot` instances (which
    represent the workload **executors** in RADICAL-Pilot) and a **scheduler**
    which determines which :class:`Task` gets executed on which
    :class:`Pilot`.

    **Example**::

        s = rp.Session(database_url=DBURL)

        pm = rp.PilotManager(session=s)

        pd = rp.PilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cores = 16

        p1 = pm.submit_pilots(pd) # create first pilot with 16 cores
        p2 = pm.submit_pilots(pd) # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' tasks
        tasks = []
        for task_count in range(0, 128):
            t = rp.TaskDescription()
            t.executable = "/bin/sleep"
            t.arguments = ['60']
            tasks.append(t)

        # Combine the two pilots, the workload and a scheduler via
        # a TaskManager.
        tm = rp.TaskManager(session=session, scheduler=rp.SCHEDULER_ROUND_ROBIN)
        tm.add_pilot(p1)
        tm.submit_tasks(tasks)


    The task manager can issue notification on task state changes.  Whenever
    state notification arrives, any callback registered for that notification is
    fired.

    NOTE: State notifications can arrive out of order wrt the task state model!
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg='default', scheduler=None):
        """
        Creates a new TaskManager and attaches it to the session.

        **Arguments:**
            * session [:class:`radical.pilot.Session`]:
              The session instance to use.
            * cfg (`dict` or `string`):
              The configuration or name of configuration to use.
            * scheduler (`string`):
              The name of the scheduler plug-in to use.

        **Returns:**
            * A new `TaskManager` object [:class:`radical.pilot.TaskManager`].
        """

        self._uid         = ru.generate_id('tmgr.%(item_counter)04d',
                                           ru.ID_CUSTOM, ns=session.uid)

        self._pilots      = dict()
        self._pilots_lock = ru.RLock('%s.pilots_lock' % self._uid)
        self._uids        = list()   # known task UIDs
        self._tasks       = dict()
        self._tasks_lock  = ru.RLock('%s.tasks_lock' % self._uid)
        self._callbacks   = dict()
        self._tcb_lock    = ru.RLock('%s.tcb_lock' % self._uid)
        self._terminate   = mt.Event()
        self._closed      = False
        self._rec_id      = 0       # used for session recording

        for m in rpc.TMGR_METRICS:
            self._callbacks[m] = dict()

        # NOTE: `name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and amed it at the same time.  This might
        #       be ok for the session, but introduces a minor API inconsistency.
        #
        name = None
        if isinstance(cfg, str):
            name = cfg
            cfg  = None

        cfg           = ru.Config('radical.pilot.tmgr', name=name, cfg=cfg)
        cfg.uid       = self._uid
        cfg.owner     = self._uid
        cfg.sid       = session.uid
        cfg.base      = session.base
        cfg.path      = session.path
        cfg.dburl     = session.dburl
        cfg.heartbeat = session.cfg.heartbeat

        if scheduler:
            # overwrite the scheduler from the config file
            cfg.scheduler = scheduler


        rpu.Component.__init__(self, cfg, session=session)
        self.start()

        self._log.info('started tmgr %s', self._uid)
        self._rep.info('<<create task manager')

        # create pmgr bridges and components, use session cmgr for that
        self._cmgr = rpu.ComponentManager(self._cfg)
        self._cmgr.start_bridges()
        self._cmgr.start_components()

        # The output queue is used to forward submitted tasks to the
        # scheduling component.
        self.register_output(rps.TMGR_SCHEDULING_PENDING,
                             rpc.TMGR_SCHEDULING_QUEUE)

        # the tmgr will also collect tasks from the agent again, for output
        # staging and finalization
        if self._cfg.bridges.tmgr_staging_output_queue:
            self._has_sout = True
            self.register_output(rps.TMGR_STAGING_OUTPUT_PENDING,
                                 rpc.TMGR_STAGING_OUTPUT_QUEUE)
        else:
            self._has_sout = False

        # register the state notification pull cb
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._state_pull_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # register callback which pulls tasks back from agent
        # FIXME: this should be a tailing cursor in the update worker
        self.register_timed_cb(self._task_pull_cb,
                               timer=self._cfg['db_poll_sleeptime'])

        # also listen to the state pubsub for task state changes
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_sub_cb)

        # let session know we exist
        self._session._register_tmgr(self)

        self._prof.prof('setup_done', uid=self._uid)
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # the manager must not carry bridge and component handles across forks
        ru.atfork(self._atfork_prepare, self._atfork_parent, self._atfork_child)


    # --------------------------------------------------------------------------
    #
    # EnTK forks, make sure we don't carry traces of children across the fork
    #
    def _atfork_prepare(self): pass
    def _atfork_parent(self) : pass
    def _atfork_child(self)  :
        self._bridges    = dict()
        self._components = dict()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        self._cmgr.close()


    # --------------------------------------------------------------------------
    #
    def close(self):
        """
        Shut down the TaskManager and all its components.
        """

        # we do not cancel tasks at this point, in case any component or pilot
        # wants to continue to progress task states, which should indeed be
        # independent from the tmgr life cycle.

        if self._closed:
            return

        self._terminate.set()
        self._rep.info('<<close task manager')

        # disable callbacks during shutdown
        with self._tcb_lock:
            self._callbacks = dict()
            for m in rpc.TMGR_METRICS:
                self._callbacks[m] = dict()

        self._cmgr.close()

        self._log.info("Closed TaskManager %s." % self._uid)

        self._closed = True
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a dictionary representation of the TaskManager object.
        """

        ret = {
            'uid': self.uid,
            'cfg': self.cfg
        }

        return ret


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """
        Returns a string representation of the TaskManager object.
        """

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def _pilot_state_cb(self, pilots, state=None):

        if self._terminate.is_set():
            return False

        # we register this callback for pilots added to this tmgr.  It will
        # specifically look out for pilots which complete, and will make sure
        # that all tasks are pulled back into tmgr control if that happens
        # prematurely.
        #
        # If we find tasks which have not completed the agent part of the task
        # state model, we declare them FAILED.  If they can be restarted, we
        # resubmit an identical task, which then will get a new task ID.  This
        # avoids state model confusion (the state model is right now expected to
        # be linear), but is not intuitive for the application (FIXME).
        #
        # FIXME: there is a race with the tmgr scheduler which may, just now,
        #        and before being notified about the pilot's demise, send new
        #        tasks to the pilot.

        # we only look into pilot states when the tmgr is still active
        # FIXME: note that there is a race in that the tmgr can be closed while
        #        we are in the cb.
        # FIXME: `self._closed` is not an `mt.Event`!
        if self._closed:
            self._log.debug('tmgr closed, ignore pilot cb %s',
                            ['%s:%s' % (p.uid, p.state) for p in pilots])
            return True

        if not isinstance(pilots, list):
            pilots = [pilots]

        for pilot in pilots:

            state = pilot.state

            if state in rps.FINAL:

                self._log.debug('pilot %s is final - pull tasks', pilot.uid)

                task_cursor = self.session._dbs._c.find({
                    'type'    : 'task',
                    'pilot'   : pilot.uid,
                    'tmgr'    : self.uid,
                    'control' : {'$in' : ['agent_pending', 'agent']}})

                if not task_cursor.count():
                    tasks = list()
                else:
                    tasks = list(task_cursor)

                self._log.debug("tasks pulled: %3d (pilot dead)", len(tasks))

                if not tasks:
                    continue

                # update the tasks to avoid pulling them again next time.
                # NOTE:  this needs not locking with the task pulling in the
                #        _task_pull_cb, as that will only pull tmgr_pending
                #        tasks.
                uids = [task['uid'] for task in tasks]

                self._session._dbs._c.update({'type'  : 'task',
                                              'uid'   : {'$in'     : uids}},
                                             {'$set'  : {'control' : 'tmgr'}},
                                             multi=True)
                to_restart = list()
                for task in tasks:

                    task['state'] = rps.FAILED
                    if not task['description'].get('restartable'):
                        self._log.debug('task %s not restartable', task['uid'])
                        continue

                    self._log.debug('task %s is  restartable', task['uid'])
                    task['restarted'] = True
                    ud = rpcud.TaskDescription(task['description'])
                    to_restart.append(ud)
                    # FIXME: increment some restart counter in the description?
                    # FIXME: reference the resulting new uid in the old task.

                if to_restart and not self._closed:
                    self._log.debug('restart %s tasks', len(to_restart))
                    restarted = self.submit_tasks(to_restart)
                    for u in restarted:
                        self._log.debug('restart task %s', u.uid)

                # final tasks are not pushed
                self.advance(tasks, publish=True, push=False)


        # keep cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def _state_pull_cb(self):

        if self._terminate.is_set():
            return False

        # pull all task states from the DB, and compare to the states we know
        # about.  If any state changed, update the task instance and issue
        # notification callbacks as needed.  Do not advance the state (again).
        # FIXME: we also pull for dead tasks.  That is not efficient...
        # FIXME: this needs to be converted into a tailed cursor in the update
        #        worker
        tasks = self._session._dbs.get_tasks(tmgr_uid=self.uid)
        self._update_tasks(tasks)

        return True


    # --------------------------------------------------------------------------
    #
    def _task_pull_cb(self):

        if self._terminate.is_set():
            return False

        # pull tasks from the agent which are about to get back
        # under tmgr control, and push them into the respective queues
        # FIXME: this should also be based on a tailed cursor
        # FIXME: Unfortunately, 'find_and_modify' is not bulkable, so we have
        #        to use 'find'.  To avoid finding the same tasks over and over
        #        again, we update the 'control' field *before* running the next
        #        find -- so we do it right here.
        task_cursor = self.session._dbs._c.find({'type'    : 'task',
                                                 'tmgr'    : self.uid,
                                                 'control' : 'tmgr_pending'})

        if not task_cursor.count():
            # no tasks whatsoever...
          # self._log.info("tasks pulled:    0")
            return True  # this is not an error

        # update the tasks to avoid pulling them again next time.
        tasks = list(task_cursor)
        uids  = [task['uid'] for task in tasks]

        self._log.info("tasks pulled:    %d", len(uids))

        for task in tasks:
            task['control'] = 'tmgr'

        self._session._dbs._c.update({'type'  : 'task',
                                      'uid'   : {'$in'     : uids}},
                                     {'$set'  : {'control' : 'tmgr'}},
                                     multi=True)

        self._log.info("tasks pulled: %4d", len(tasks))
        self._prof.prof('get', msg="bulk size: %d" % len(tasks), uid=self.uid)
        for task in tasks:

            # we need to make sure to have the correct state:
            uid = task['uid']
            self._prof.prof('get', uid=uid)

            old = task['state']
            new = rps._task_state_collapse(task['states'])

            if old != new:
                self._log.debug("task  pulled %s: %s / %s", uid, old, new)

            task['state'] = new

        # now we really own the CUs, and can start working on them (ie. push
        # them into the pipeline).

        to_stage    = list()
        to_finalize = list()

        for task in tasks:
            # only advance tasks to data stager if we need data staging
            # = otherwise finalize them right away
            if task['description'].get('output_staging'):
                to_stage.append(task)
            else:
                to_finalize.append(task)

        # don't profile state transitions - those happened in the past
        if to_stage:
            if self._has_sout:
                # normal route: needs data stager
                self.advance(to_stage, publish=True, push=True, prof=False)
            else:
                self._log.error('output staging needed but not available!')
                for task in to_stage:
                    task['target_state'] = rps.FAILED
                    to_finalize.append(task)

        if to_finalize:
            # shortcut, skip the data stager, but fake state transition
            self.advance(to_finalize, state=rps.TMGR_STAGING_OUTPUT,
                                      publish=True, push=False)

            # move to final stata
            for task in to_finalize:
                task['state'] = task['target_state']
            self.advance(to_finalize, publish=True, push=False)

        return True


    # --------------------------------------------------------------------------
    #
    def _state_sub_cb(self, topic, msg):

        if self._terminate.is_set():
            return False

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd != 'update':
            self._log.debug('ignore state cb msg with cmd %s', cmd)
            return True

        things = ru.as_list(arg)
        tasks  = [thing for thing in things if thing.get('type') == 'task']

        self._update_tasks(tasks)

        return True


    # --------------------------------------------------------------------------
    #
    def _update_tasks(self, task_dicts):


        # return information about needed callback and advance activities, so
        # that we don't break bulks here.
        # note however that individual task callbacks are still being called on
        # each task (if any are registered), which can lead to arbitrary,
        # application defined delays.

        to_notify = list()

        with self._tasks_lock:

            for task_dict in task_dicts:

                uid = task_dict['uid']

                # we don't care about tasks we don't know
                task = self._tasks.get(uid)
                if not task:
                    self._log.debug('tmgr: task unknown: %s', uid)
                    continue

                # only update on state changes
                current = task.state
                target  = task_dict['state']
                if current == target:
                    continue

                target, passed = rps._task_state_progress(uid, current, target)

                if target in [rps.CANCELED, rps.FAILED]:
                    # don't replay intermediate states
                    passed = passed[-1:]

                for s in passed:
                    task_dict['state'] = s
                    self._tasks[uid]._update(task_dict)
                    to_notify.append([task, s])

        if to_notify:
            if _USE_BULK_CB:
                self._bulk_cbs(set([task for task,_ in to_notify]))
            else:
                for task, state in to_notify:
                    self._task_cb(task, state)


    # --------------------------------------------------------------------------
    #
    def _task_cb(self, task, state):

        with self._tcb_lock:

            uid      = task.uid
            cb_dicts = list()
            metric   = rpc.TASK_STATE

            # get wildcard callbacks
            cb_dicts += self._callbacks[metric].get('*', {}).values()
            cb_dicts += self._callbacks[metric].get(uid, {}).values()

            for cb_dict in cb_dicts:

                cb      = cb_dict['cb']
                cb_data = cb_dict['cb_data']

                try:
                    if cb_data: cb(task, state, cb_data)
                    else      : cb(task, state)
                except:
                    self._log.exception('cb error (%s)', cb.__name__)


    # --------------------------------------------------------------------------
    #
    def _bulk_cbs(self, tasks,  metrics=None):

        if not metrics: metrics = [rpc.TASK_STATE]
        else          : metrics = ru.as_list(metrics)

        cbs = dict()  # bulked callbacks to call

        with self._tcb_lock:

            for metric in metrics:

                # get wildcard callbacks
                cb_dicts = self._callbacks[metric].get('*')
                for cb_name in cb_dicts:
                    cbs[cb_name] = {'cb'     : cb_dicts[cb_name]['cb'],
                                    'cb_data': cb_dicts[cb_name]['cb_data'],
                                    'tasks'  : set(tasks)}

                # add task specific callbacks if needed
                for task in tasks:

                    uid = task.uid
                    if uid not in self._callbacks[metric]:
                        continue

                    cb_dicts = self._callbacks[metric].get(uid, {})
                    for cb_name in cb_dicts:

                        if cb_name in cbs:
                            cbs[cb_name]['tasks'].add(task)
                        else:
                            cbs[cb_name] = {'cb'     : cb_dicts[cb_name]['cb'],
                                            'cb_data': cb_dicts[cb_name]['cb_data'],
                                            'tasks'  : set([task])}

            for cb_name in cbs:

                cb      = cbs[cb_name]['cb']
                cb_data = cbs[cb_name]['cb_data']
                objs    = cbs[cb_name]['tasks']

                if cb_data: cb(list(objs), cb_data)
                else      : cb(list(objs))


    # --------------------------------------------------------------------------
    #
    # FIXME: this needs to go to the scheduler
    def _default_wait_queue_size_cb(self, tmgr, wait_queue_size):

        # FIXME: this needs to come from the scheduler?
        if self._terminate.is_set():
            return False

        self._log.info("[Callback]: wait_queue_size: %s.", wait_queue_size)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """
        Returns the unique id.
        """
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def scheduler(self):
        """
        Returns the scheduler name.
        """

        return self._cfg.get('scheduler')



    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """
        Associates one or more pilots with the task manager.

        **Arguments:**

            * **pilots** [:class:`radical.pilot.Pilot` or list of
              :class:`radical.pilot.Pilot`]: The pilot objects that will be
              added to the task manager.
        """

        if not isinstance(pilots, list):
            pilots = [pilots]

        if len(pilots) == 0:
            raise ValueError('cannot add no pilots')

        pilot_docs = list()
        with self._pilots_lock:

            # sanity check, and keep pilots around for inspection
            for pilot in pilots:

                if isinstance(pilot, dict):
                    pilot_dict = pilot
                else:
                    pilot_dict = pilot.as_dict()
                    # real object: subscribe for state updates
                    pilot.register_callback(self._pilot_state_cb)

                pid = pilot_dict['uid']
                if pid in self._pilots:
                    raise ValueError('pilot %s already added' % pid)
                self._pilots[pid] = pilot_dict
                pilot_docs.append(pilot_dict)

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'add_pilots',
                                          'arg' : {'pilots': pilot_docs,
                                                   'tmgr'  : self.uid}})


    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """
        Lists the UIDs of the pilots currently associated with the task manager.

        **Returns:**
              * A list of :class:`radical.pilot.Pilot` UIDs [`string`].
        """

        with self._pilots_lock:
            return list(self._pilots.keys())


    # --------------------------------------------------------------------------
    #
    def get_pilots(self):
        """
        Get the pilots instances currently associated with the task manager.

        **Returns:**
              * A list of :class:`radical.pilot.Pilot` instances.
        """

        with self._pilots_lock:
            return list(self._pilots.values())


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_ids, drain=False):
        """
        Disassociates one or more pilots from the task manager.

        After a pilot has been removed from a task manager, it won't process
        any of the task manager's tasks anymore. Calling `remove_pilots`
        doesn't stop the pilot itself.

        **Arguments:**

            * **drain** [`boolean`]: Drain determines what happens to the tasks
              which are managed by the removed pilot(s). If `True`, all tasks
              currently assigned to the pilot are allowed to finish execution.
              If `False` (the default), then non-final tasks will be canceled.
        """

        # TODO: Implement 'drain'.
        # NOTE: the actual removal of pilots from the scheduler is asynchron!

        if drain:
            raise RuntimeError("'drain' is not yet implemented")

        if not isinstance(pilot_ids, list):
            pilot_ids = [pilot_ids]

        if len(pilot_ids) == 0:
            raise ValueError('cannot remove no pilots')

        with self._pilots_lock:

            # sanity check, and keep pilots around for inspection
            for pid in pilot_ids:
                if pid not in self._pilots:
                    raise ValueError('pilot %s not removed' % pid)
                del(self._pilots[pid])

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'remove_pilots',
                                          'arg' : {'pids'  : pilot_ids,
                                                   'tmgr'  : self.uid}})


    # --------------------------------------------------------------------------
    #
    def list_units(self):
        '''
        deprecated - use `list_tasks()`
        '''
        _warn(self.list_units, self.list_tasks)
        return self.list_tasks()


    # --------------------------------------------------------------------------
    #
    def list_tasks(self):
        """
        Returns the UIDs of the :class:`radical.pilot.Task` managed by
        this task manager.

        **Returns:**
              * A list of :class:`radical.pilot.Task` UIDs [`string`].
        """

        with self._pilots_lock:
            return list(self._tasks.keys())


    # --------------------------------------------------------------------------
    #
    def submit_units(self, descriptions):
        '''
        deprecated - use `submit_tasks()`
        '''
        _warn(self.submit_units, self.submit_tasks)
        return self.submit_tasks(descriptions=descriptions)


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions):
        """
        Submits on or more :class:`radical.pilot.Task` instances to the
        task manager.

        **Arguments:**
            * **descriptions** [:class:`radical.pilot.TaskDescription`
              or list of :class:`radical.pilot.TaskDescription`]: The
              description of the task instance(s) to create.

        **Returns:**
              * A list of :class:`radical.pilot.Task` objects.
        """

        from .task import Task

        ret_list = True
        if not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        if len(descriptions) == 0:
            raise ValueError('cannot submit no task descriptions')

        # we return a list of tasks
        self._rep.progress_tgt(len(descriptions), label='submit')
        tasks = list()
        for ud in descriptions:

            if not ud.executable:
                raise ValueError('task executable must be defined')

            task = Task(tmgr=self, descr=ud)
            tasks.append(task)

            # keep tasks around
            with self._tasks_lock:
                self._tasks[task.uid] = task

            if self._session._rec:
                ru.write_json(ud.as_dict(), "%s/%s.batch.%03d.json"
                        % (self._session._rec, task.uid, self._rec_id))

            self._rep.progress()

        self._rep.progress_done()

        if self._session._rec:
            self._rec_id += 1

        # insert tasks into the database, as a bulk.
        task_docs = [u.as_dict() for u in tasks]
        self._session._dbs.insert_tasks(task_docs)

        # Only after the insert can we hand the tasks over to the next
        # components (ie. advance state).
        self.advance(task_docs, rps.TMGR_SCHEDULING_PENDING,
                     publish=True, push=True)

        if ret_list: return tasks
        else       : return tasks[0]


    # --------------------------------------------------------------------------
    #
    def get_units(self, uids=None):
        '''
        deprecated - use `get_tasks()`
        '''
        _warn(self.get_units, self.get_tasks)
        return self.get_tasks(uids=uids)


    # --------------------------------------------------------------------------
    #
    def get_tasks(self, uids=None):
        """Returns one or more tasks identified by their IDs.

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              task objects to return.

        **Returns:**
              * A list of :class:`radical.pilot.Task` objects.
        """

        if not uids:
            with self._tasks_lock:
                ret = list(self._tasks.values())
            return ret

        ret_list = True
        if (not isinstance(uids, list)) and (uids is not None):
            ret_list = False
            uids = [uids]

        ret = list()
        with self._tasks_lock:
            for uid in uids:
                if uid not in self._tasks:
                    raise ValueError('task %s not known' % uid)
                ret.append(self._tasks[uid])

        if ret_list: return ret
        else       : return ret[0]


    # --------------------------------------------------------------------------
    #
    def wait_units(self, uids=None, state=None, timeout=None):
        '''
        deprecated - use `wait_tasks()`
        '''
        _warn(self.wait_units, self.wait_tasks)
        return self.wait_tasks(uids=uids, state=state, timeout=timeout)


    # --------------------------------------------------------------------------
    #
    def wait_tasks(self, uids=None, state=None, timeout=None):
        """
        Returns when one or more :class:`radical.pilot.Tasks` reach a
        specific state.

        If `uids` is `None`, `wait_tasks` returns when **all**
        Tasks reach the state defined in `state`.  This may include
        tasks which have previously terminated or waited upon.

        **Example**::

            # TODO -- add example

        **Arguments:**

            * **uids** [`string` or `list of strings`]
              If uids is set, only the Tasks with the specified
              uids are considered. If uids is `None` (default), all
              Tasks are considered.

            * **state** [`string`]
              The state that Tasks have to reach in order for the call
              to return.

              By default `wait_tasks` waits for the Tasks to
              reach a terminal state, which can be one of the following:

              * :data:`radical.pilot.rps.DONE`
              * :data:`radical.pilot.rps.FAILED`
              * :data:`radical.pilot.rps.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.
        """

        if not uids:
            with self._tasks_lock:
                uids = list()
                for uid,task in self._tasks.items():
                    if task.state not in rps.FINAL:
                        uids.append(uid)

        if   not state                  : states = rps.FINAL
        elif not isinstance(state, list): states = [state]
        else                            : states =  state

        # we simplify state check by waiting for the *earliest* of the given
        # states - if the task happens to be in any later state, we are sure the
        # earliest has passed as well.
        check_state_val = rps._task_state_values[rps.FINAL[-1]]
        for state in states:
            check_state_val = min(check_state_val,
                                  rps._task_state_values[state])

        ret_list = True
        if not isinstance(uids, list):
            ret_list = False
            uids = [uids]

        start    = time.time()
        to_check = None

        with self._tasks_lock:
            to_check = [self._tasks[uid] for uid in uids]

        # We don't want to iterate over all tasks again and again, as that would
        # duplicate checks on tasks which were found in matching states.  So we
        # create a list from which we drop the tasks as we find them in
        # a matching state
        self._rep.progress_tgt(len(to_check), label='wait')
        while to_check and not self._terminate.is_set():

            # check timeout
            if timeout and (timeout <= (time.time() - start)):
                self._log.debug ("wait timed out")
                break

            time.sleep (0.1)

            # FIXME: print percentage...
          # print 'wait tasks: %s' % [[u.uid, u.state] for u in to_check]

            check_again = list()
            for task in to_check:

                # we actually don't check if a task is in a specific (set of)
                # state(s), but rather check if it ever *has been* in any of
                # those states
                if task.state not in rps.FINAL and \
                    rps._task_state_values[task.state] < check_state_val:
                    # this task does not match the wait criteria
                    check_again.append(task)

                else:
                    # stop watching this task
                    if task.state in [rps.FAILED]:
                        self._rep.progress()  # (color='error', c='-')
                    elif task.state in [rps.CANCELED]:
                        self._rep.progress()  # (color='warn', c='*')
                    else:
                        self._rep.progress()  # (color='ok', c='+')

            to_check = check_again

        self._rep.progress_done()


        # grab the current states to return
        state = None
        with self._tasks_lock:
            states = [self._tasks[uid].state for uid in uids]

        sdict = {state: states.count(state) for state in set(states)}
        for state in sorted(set(states)):
            self._rep.info('\t%-10s: %5d\n' % (state, sdict[state]))

        if to_check: self._rep.warn('>>timeout\n')
        else       : self._rep.ok  ('>>ok\n')

        # done waiting
        if ret_list: return states
        else       : return states[0]


    # --------------------------------------------------------------------------
    #
    def cancel_units(self, uids=None):
        '''
        deprecated - use `cancel_tasks()`
        '''
        _warn(self.cancel_units, self.cancel_tasks)
        return self.cancel_tasks(uids=uids)


    # --------------------------------------------------------------------------
    #
    def cancel_tasks(self, uids=None):
        """
        Cancel one or more :class:`radical.pilot.Tasks`.

        Note that cancellation of tasks is *immediate*, i.e. their state is
        immediately set to `CANCELED`, even if some RP component may still
        operate on the tasks.  Specifically, other state transitions, including
        other final states (`DONE`, `FAILED`) can occur *after* cancellation.
        This is a side effect of an optimization: we consider this
        acceptable tradeoff in the sense "Oh, that task was DONE at point of
        cancellation -- ok, we can use the results, sure!".

        If that behavior is not wanted, set the environment variable:

            export RADICAL_PILOT_STRICT_CANCEL=True

        **Arguments:**
            * **uids** [`string` or `list of strings`]: The IDs of the
              tasks objects to cancel.
        """

        if not uids:
            with self._tasks_lock:
                uids  = list(self._tasks.keys())
        else:
            if not isinstance(uids, list):
                uids = [uids]

        # NOTE: We advance all tasks to cancelled, and send a cancellation
        #       control command.  If that command is picked up *after* some
        #       state progression, we'll see state transitions after cancel.
        #       For non-final states that is not a problem, as it is equivalent
        #       with a state update message race, which our state collapse
        #       mechanism accounts for.  For an eventual non-canceled final
        #       state, we do get an invalid state transition.  That is also
        #       corrected eventually in the state collapse, but the point
        #       remains, that the state model is temporarily violated.  We
        #       consider this a side effect of the fast-cancel optimization.
        #
        #       The env variable 'RADICAL_PILOT_STRICT_CANCEL == True' will
        #       disable this optimization.
        #
        # FIXME: the effect of the env var is not well tested
        if 'RADICAL_PILOT_STRICT_CANCEL' not in os.environ:
            with self._tasks_lock:
                tasks = [self._tasks[uid] for uid  in uids ]
            task_docs = [task.as_dict()   for task in tasks]
            self.advance(task_docs, state=rps.CANCELED, publish=True, push=True)

        # we *always* issue the cancellation command to the local components
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_tasks',
                                          'arg' : {'uids' : uids,
                                                   'tmgr' : self.uid}})

        # we also inform all pilots about the cancelation request
        self._session._dbs.pilot_command(cmd='cancel_tasks', arg={'uids':uids})

        # In the default case of calling 'advance' above, we just set the state,
        # so we *know* tasks are canceled.  But we nevertheless wait until that
        # state progression trickled through, so that the application will see
        # the same state on task inspection.
        self.wait_tasks(uids=uids)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, cb_data=None, metric=None, uid=None):
        """
        Registers a new callback function with the TaskManager.  Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `TASK_STATE` fires the callback if any of the Tasks
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb(obj, value)

        where ``object`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `TASK_STATE` above, the
        object would be the task in question, and the value would be the new
        state of the task.

        If 'cb_data' is given, then the 'cb' signature changes to

            def cb(obj, state, cb_data)

        and 'cb_data' are passed unchanged.

        If 'uid' is given, the callback will invoked only for the specified
        task.


        Available metrics are:

          * `TASK_STATE`: fires when the state of any of the tasks which are
            managed by this task manager instance is changing.  It communicates
            the task object instance and the tasks new state.

          * `WAIT_QUEUE_SIZE`: fires when the number of unscheduled tasks (i.e.
            of tasks which have not been assigned to a pilot for execution)
            changes.
        """

        # FIXME: the signature should be (self, metrics, cb, cb_data)

        if not metric:
            metric = rpc.TASK_STATE

        if  metric not in rpc.TMGR_METRICS:
            raise ValueError ("Metric '%s' not available on the tmgr" % metric)

        if not uid:
            uid = '*'

        elif uid not in self._tasks:
            raise ValueError('no such task %s' % uid)


        with self._tcb_lock:
            cb_name = cb.__name__

            if metric not in self._callbacks:
                self._callbacks[metric] = dict()

            if uid not in self._callbacks[metric]:
                self._callbacks[metric][uid] = dict()

            self._callbacks[metric][uid][cb_name] = {'cb'      : cb,
                                                     'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb=None, metrics=None, uid=None):

        if not metrics: metrics = [rpc.TMGR_METRICS]
        else          : metrics = ru.as_list(metrics)

        if not uid:
            uid = '*'

        elif uid not in self._tasks:
            raise ValueError('no such task %s' % uid)

        for metric in metrics:
            if metric not in rpc.TMGR_METRICS :
                raise ValueError ("invalid tmgr metric '%s'" % metric)

        with self._tcb_lock:

            for metric in metrics:

                if metric not in rpc.TMGR_METRICS :
                    raise ValueError("cb metric '%s' unknown" % metric)

                if metric not in self._callbacks:
                    raise ValueError("cb metric '%s' invalid" % metric)

                if uid not in self._callbacks[metric]:
                    raise ValueError("cb target '%s' invalid" % uid)

                if cb:
                    to_delete = [cb.__name__]
                else:
                    to_delete = list(self._callbacks[metric][uid].keys())

                for cb_name in to_delete:

                    if cb_name not in self._callbacks[uid][metric]:
                        raise ValueError("cb %s not registered" % cb_name)

                    del(self._callbacks[uid][metric][cb_name])


    # --------------------------------------------------------------------------
    #
    def check_uid(self, uid):

        # ensure that uid is not yet known
        if uid in self._uids:
            return False
        else:
            self._uids.append(uid)
            return True


# ------------------------------------------------------------------------------

