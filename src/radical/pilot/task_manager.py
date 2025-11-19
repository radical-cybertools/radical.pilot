
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import time
import queue
import collections

import threading     as mt

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc

from .task_description import RAPTOR_MASTER, RAPTOR_WORKER
from .raptor_tasks     import RaptorMaster, RaptorWorker

_DEFAULT_SUBMIT_BULK_SIZE = 1024 * 1024


# bulk callbacks are implemented, but are currently not used nor exposed.
_USE_BULK_CB = False
if os.environ.get('RADICAL_PILOT_BULK_CB', '').lower() in ['true', 'yes', '1']:
    _USE_BULK_CB = True


# ------------------------------------------------------------------------------
#
class TaskManager(rpu.ClientComponent):
    """
    A TaskManager manages :class:`radical.pilot.Task` instances which
    represent the **executable** workload in RADICAL-Pilot. A TaskManager
    connects the Tasks with one or more :class:`Pilot` instances (which
    represent the workload **executors** in RADICAL-Pilot) and a **scheduler**
    which determines which :class:`Task` gets executed on which
    :class:`Pilot`.

    Example::

        s = rp.Session()

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

    Note:
        State notifications can arrive out of order wrt the task state model!

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg='default', scheduler=None):
        """Create a new TaskManager and attaches it to the session.

        Arguments:
            session (radical.pilot.Session): The session instance to use.
            cfg (dict | str): The configuration or name of configuration to use.
            scheduler (str): The name of the scheduler plug-in to use.
            uid (str): ID for unit manager, to be used for reconnect

        Returns:
            radical.pilot.TaskManager: A new `TaskManager` object.

        """


        assert session._role == session._PRIMARY, 'tmgr needs primary session'

        # initialize the base class (with no intent to fork)
        self._uid = ru.generate_id('tmgr.%(item_counter)04d',
                                    ru.ID_CUSTOM, ns=session.uid)

        if not scheduler:
            scheduler = rpc.SCHEDULER_ROUND_ROBIN

        self._known_uids  = set()
        self._pilots      = dict()
        self._pilots_lock = mt.RLock()
        self._tasks       = dict()
        self._tasks_lock  = mt.RLock()
        self._callbacks   = dict()
        self._tcb_lock    = mt.RLock()
        self._terminate   = mt.Event()
        self._closed      = False
        self._task_info   = collections.defaultdict(dict)

        for m in rpc.TMGR_METRICS:
            self._callbacks[m] = dict()

        # NOTE: `name` and `cfg` are overloaded, the user cannot point to
        #       a predefined config and name it at the same time.  This might
        #       be ok for the session, but introduces a minor API inconsistency.
        #
        name = None
        if isinstance(cfg, str):
            name = cfg
            cfg  = None

        cfg                = ru.Config('radical.pilot.tmgr', name=name, cfg=cfg)
        cfg.uid            = self._uid
        cfg.owner          = self._uid
        cfg.sid            = session.uid
        cfg.path           = session.path
        cfg.reg_addr       = session.reg_addr
        cfg.heartbeat      = session.cfg.heartbeat
        cfg.client_sandbox = session._get_client_sandbox()

        super().__init__(cfg, session=session)
        self.start()

        self._log.info('started tmgr %s', self._uid)

        self._rep = self._session._get_reporter(name=self._uid)
        self._rep.info('<<create task manager')

        # overwrite the scheduler from the config file
        if 'tmgr_scheduling' in self._cfg.components:
            self._cfg.components.tmgr_scheduling.scheduler = scheduler

        # create pmgr bridges and components, use session cmgr for that
        self._cmgr = rpu.ComponentManager(cfg.sid, cfg.reg_addr, self._uid)
        self._cmgr.start_bridges(self._cfg.bridges)
        self._cmgr.start_components(self._cfg.components)

        # let session know we exist
        self._session._register_tmgr(self)

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

        # also listen to the state pubsub for task state changes
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_sub_cb)

        # hook into the control pubsub for rpc handling
        self._rpc_queue = queue.Queue()
        ctrl_addr_pub   = self._session._reg['bridges.control_pubsub.addr_pub']
        ctrl_addr_sub   = self._session._reg['bridges.control_pubsub.addr_sub']

        self._ctrl_pub  = ru.zmq.Publisher(rpc.CONTROL_PUBSUB, url=ctrl_addr_pub,
                                           log=self._log, prof=self._prof)

        self._ctrl_sub = ru.zmq.Subscriber(rpc.CONTROL_PUBSUB, url=ctrl_addr_sub,
                                           log=self._log, prof=self._prof,
                                           cb=self._control_cb,
                                           topic=rpc.CONTROL_PUBSUB)

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
        """Shut down the TaskManager and all its components."""

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

        self.dump()

        self._ctrl_sub.stop()

        super().close()


    # --------------------------------------------------------------------------
    #
    def dump(self, name=None):

        # dump json
        json = self.as_dict()
      # json['_id']   = self.uid
        json['type']  = 'tmgr'
        json['uid']   = self.uid
        json['tasks'] = self._task_info

        if name:
            tgt = '%s/%s.%s.json' % (self._session.path, self.uid, name)
        else:
            tgt = '%s/%s.json' % (self._session.path, self.uid)

        ru.write_json(json, tgt)

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a dictionary representation of the TaskManager object."""

        ret = {
            'uid': self.uid,
            'cfg': self.cfg
        }

        return ret


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the TaskManager object."""

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

            pid   = pilot.uid
            state = pilot.state

            if state in rps.FINAL:

                self._log.debug('pilot %s is final', pid)

                tasks = list()
                for task in self._tasks.values():

                    update = {'uid'             : task.uid,
                              'exception'       : 'RuntimeError("pilot died")',
                              'exception_detail': 'pilot %s is final' % pid,
                              'state'           : rps.FAILED}

                    task._update(update)
                    tasks.append(task.as_dict())

                # final tasks are not pushed
                self.advance(tasks, publish=True, push=False)

        # keep cb registered
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

                self._log.debug('task update: %s: %s [%s]',
                                task_dict['uid'], task_dict['state'],
                                task_dict.get('target_state'))

                uid = task_dict['uid']

                # we don't care about tasks we don't know
                task = self._tasks.get(uid)
                if not task:
                    self._log.debug('tmgr: task unknown: %s', uid)
                    continue

                # only update on state changes
                current = task.state
                target  = task_dict['state']

              # if 'target_state' in task_dict:
              #     target = task_dict['target_state']

                if current == target:
                    self._log.debug('tmgr: state known: %s', uid)
                    continue

                target, passed = rps._task_state_progress(uid, current, target)

                if target in [rps.CANCELED, rps.FAILED]:
                    # don't replay intermediate states
                    passed = passed[-1:]

                for s in passed:

                    task_dict['state'] = s
                    self._tasks[uid]._update(task_dict)

                    to_notify.append([task, s])

                task_dict['state'] = self._tasks[uid].state
                ru.dict_merge(self._task_info[uid], task_dict, ru.OVERWRITE)

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
        """str: The unique id."""
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def scheduler(self):
        """str: The scheduler name."""

        return self._cfg.get('scheduler')



    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """Associates one or more pilots with the task manager.

        Arguments:
            pilots (radical.pilot.Pilot | list[radical.pilot.Pilot]):
                The pilot objects that will be added to the task manager.

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
                    # let pilot know we own it and subscribe for state updates
                    # FIXME: this is not working for pilot dicts (ENTK)
                    pilot.attach_tmgr(self)

                    pilot_dict = pilot.as_dict()
                    pilot.register_callback(self._pilot_state_cb)

                pid = pilot_dict['uid']

                if pid in self._pilots:
                    raise ValueError('pilot %s already added' % pid)
                self._pilots[pid] = pilot
                pilot_docs.append(pilot_dict)

        # publish to the command channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'add_pilots',
                                          'arg' : {'pilots': pilot_docs,
                                                   'tmgr'  : self.uid}})


    # --------------------------------------------------------------------------
    #
    def list_pilots(self):
        """Lists the UIDs of the pilots currently associated with the task manager.

        Returns:
            list[str]: A list of :class:`radical.pilot.Pilot` UIDs.

        """

        with self._pilots_lock:
            return list(self._pilots.keys())


    # --------------------------------------------------------------------------
    #
    def get_pilots(self):
        """Get the pilot instances currently associated with the task manager.

        Returns:
            list[radical.pilot.Pilot]: A list of :class:`radical.pilot.Pilot` instances.

        """

        with self._pilots_lock:
            return list(self._pilots.values())


    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_ids, drain=False):
        """Disassociates one or more pilots from the task manager.

        After a pilot has been removed from a task manager, it won't process
        any of the task manager's tasks anymore. Calling `remove_pilots`
        doesn't stop the pilot itself.

        Arguments:
            drain (bool): Drain determines what happens to the tasks
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
                del self._pilots[pid]

        # publish to the control channel for the scheduler to pick up
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'remove_pilots',
                                          'arg' : {'pids'  : pilot_ids,
                                                   'tmgr'  : self.uid}})


    # --------------------------------------------------------------------------
    #
    # FIXME RPC
    def _control_cb(self, topic, msg):

        self._log.debug_5('control cb: %s', msg)

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'rpc_res':

            self._log.debug('rpc res: %s', arg)
            self._rpc_queue.put(arg)


        elif cmd == 'service_up':

            self._log.debug('service up: %s', arg)
            self._service_update(arg)


    # --------------------------------------------------------------------------
    #
    def _service_update(self, arg):

        uid  = arg['uid']
        task = self._tasks.get(uid)

        if not task:
            return

        task._set_info(arg['info'] or '')  # `None` would indicate an error
        self._log.debug('service update: %s: %s', uid, task.info)


    # --------------------------------------------------------------------------
    #
    def pilot_rpc(self, pid, cmd, *args, rpc_addr=None, **kwargs):
        '''Remote procedure call.

        Send an RPC command and arguments to the pilot and wait for the
        response.  This is a synchronous operation at this point, and it is not
        thread safe to have multiple concurrent RPC calls.
        '''

        if pid not in self._pilots:
            raise ValueError('tmgr does not know pilot %s' % pid)

        return self._pilots[pid].rpc(cmd, *args, rpc_addr=rpc_addr, **kwargs)


    # --------------------------------------------------------------------------
    #
    def list_tasks(self):
        """Get the UIDs of the tasks managed by this task manager.

        Returns:
            list[str]: A list of :class:`radical.pilot.Task` UIDs.

        """

        with self._pilots_lock:
            return list(self._tasks.keys())


    # --------------------------------------------------------------------------
    #
    def submit_raptors(self, descriptions, pilot_id=None):
        """Submit RAPTOR master tasks.

        Submits on or more :class:`radical.pilot.TaskDescription` instances to
        the task manager, where the `TaskDescriptions` have the mode
        `RAPTOR_MASTER` set.

        This is a thin wrapper around `submit_tasks`.

        Arguments:
            descriptions: (radical.pilot.TaskDescription) description of the
                workers to submit.

        Returns:
            list[radical.pilot.Task]: A list of :class:`radical.pilot.Task`
                objects.
        """

        descriptions = ru.as_list(descriptions)

        for td in descriptions:

            if not td.mode == RAPTOR_MASTER:
                raise ValueError('unexpected task mode [%s]' % td.mode)

            raptor_file  = td.get('raptor_file')  or  ''
            raptor_class = td.get('raptor_class') or  'Master'

            td.pilot     = pilot_id
            td.arguments = [raptor_file, raptor_class]

            td.environment['PYTHONUNBUFFERED'] = '1'

            if not td.get('uid'):
                td.uid = ru.generate_id('raptor.%(item_counter)04d',
                                        ru.ID_CUSTOM, ns=self._session.uid)

            if not td.get('executable'):
                td.executable = 'radical-pilot-raptor-master'

            if not td.get('named_env'):
                td.named_env = 'rp'

            # ensure that defaults and backward compatibility kick in
            td.verify()

        return self.submit_tasks(descriptions)


    # --------------------------------------------------------------------------
    #
    def submit_workers(self, descriptions):
        """Submit RAPTOR workers.

        Submits on or more :class:`radical.pilot.TaskDescription` instances to
        the task manager, where the `TaskDescriptions` have the mode
        `RAPTOR_WORKER` set.

        This method is a thin wrapper around `submit_tasks`.

        Arguments:
            descriptions: (radical.pilot.TaskDescription) description of the
                workers to submit.

        Returns:
            list[radical.pilot.Task]: A list of :class:`radical.pilot.Task`
                objects.
        """

        descriptions = ru.as_list(descriptions)

        for td in descriptions:

            if not td.mode == RAPTOR_WORKER:
                raise ValueError('unexpected task mode [%s]' % td.mode)

            raptor_id    = td.get('raptor_id')
            raptor_file  = td.get('raptor_file')  or  ''
            raptor_class = td.get('raptor_class') or  'DefaultWorker'

            if not raptor_id:
                raise ValueError('RAPTOR_WORKER descriptions need `raptor_id`')

            if not td.get('uid'):
                td.uid = ru.generate_id(raptor_id + '.%(item_counter)04d',
                                        ru.ID_CUSTOM, ns=self._session.uid)

            if not td.get('executable'):
                td.executable = 'radical-pilot-raptor-worker'

            if not td.get('named_env'):
                td.named_env = 'rp'

            td.raptor_id = raptor_id
            td.arguments = [raptor_file, raptor_class, raptor_id]

            td.environment['PYTHONUNBUFFERED'] = '1'

            # ensure that defaults and backward compatibility kick in
            td.verify()

        return self.submit_tasks(descriptions)


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions):
        """Submit tasks for execution.

        Submits one or more :class:`radical.pilot.Task` instances to the task
        manager.

        Arguments:
            descriptions (radical.pilot.TaskDescription | list
                [radical.pilot.TaskDescription]):
                The description of the task instance(s) to create.

        Returns:
            list[radical.pilot.Task]: A list of :class:`radical.pilot.Task`
                objects.

        """

        from .task import Task

        if not descriptions:
            return []

        ret_list = True
        if descriptions and not isinstance(descriptions, list):
            ret_list     = False
            descriptions = [descriptions]

        # we don't want the submission to stall just because some tasks get
        # completed meanwhile, so we lock the submission routine.
        with self._tasks_lock:

            # we return a list of tasks
            tasks = list()
            ret   = list()
            self._rep.progress_tgt(len(descriptions), label='submit')
            for td in descriptions:

                # ensure uid is unique
                if not td.uid:

                    while True:
                        td.uid = ru.generate_id('task.%(item_counter)06d',
                                                ru.ID_CUSTOM, ns=self._session.uid)
                        if self._check_uid(td.uid):
                            break


                else:
                    if not self._check_uid(td.uid):
                        raise ValueError('uid %s is not unique' % td.uid)

                mode = td.mode

                if mode == RAPTOR_MASTER:
                    task = RaptorMaster(tmgr=self, descr=td, origin='client')

                elif mode == RAPTOR_WORKER:
                    task = RaptorWorker(tmgr=self, descr=td, origin='client')

                else:
                    task = Task(tmgr=self, descr=td, origin='client')

                tasks.append(task)
                self._rep.progress()

                if len(tasks) >= _DEFAULT_SUBMIT_BULK_SIZE:

                    # submit this bulk
                    for task in tasks:
                        self._tasks[task.uid] = task

                    task_docs = [u.as_dict() for u in tasks]
                    self.advance(task_docs, rps.TMGR_SCHEDULING_PENDING,
                                 publish=True, push=True)
                    ret += tasks
                    tasks = list()

            # submit remaining bulk (if any)
            if tasks:
                for task in tasks:
                    self._tasks[task.uid] = task

                task_docs = [t.as_dict() for t in tasks]
                self.advance(task_docs, rps.TMGR_SCHEDULING_PENDING,
                             publish=True, push=True)
                ret += tasks

        self._rep.progress_done()

        if ret_list: return ret
        else       : return ret[0]


    # ------------------------------------------------------------------------------
    #
    def _check_uid(self, uid):

        # ensure that uid is unique
        if uid in self._known_uids:
            return False
        else:
            self._known_uids.add(uid)
            return True


    # --------------------------------------------------------------------------
    #
    def get_tasks(self, uids=None):
        """Get one or more tasks identified by their IDs.

        Arguments:
            uids (str | list[str]): The IDs of the task objects to return.

        Returns:
            list[radical.pilot.Task]:
                A list of :class:`radical.pilot.Task` objects.

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
    def wait_tasks(self, uids=None, state=None, timeout=None):
        """Block for state transition.

        Returns when one or more :class:`radical.pilot.Tasks` reach a
        specific state.

        If `uids` is `None`, `wait_tasks` returns when **all**
        Tasks reach the state defined in `state`.  This may include
        tasks which have previously terminated or waited upon.

        Example::

            # TODO -- add example

        Arguments:
            uids (str | list[str]): If uids is set, only the Tasks with the
                specified uids are considered. If uids is `None` (default), all
                Tasks are considered.
            state (str): The state that Tasks have to reach in order for the call
                to return.

                By default `wait_tasks` waits for the Tasks to
                reach a terminal state, which can be one of the following.

                * :data:`radical.pilot.rps.DONE`
                * :data:`radical.pilot.rps.FAILED`
                * :data:`radical.pilot.rps.CANCELED`
            timeout (float):
                Timeout in seconds before the call returns regardless of Pilot
                state changes. The default value **None** waits forever.

        """

        ret_list = True
        if not uids:
            with self._tasks_lock:
                uids = self._tasks.keys()
        else:
            if not isinstance(uids, list):
                ret_list = False
                uids = [uids]

        if   not state                  : states = rps.FINAL
        elif not isinstance(state, list): states = [state]
        else                            : states =  state

        self._log.debug('wait for %s: %s', uids, states)

        # we simplify state check by waiting for the *earliest* of the given
        # states - if the task happens to be in any later state, we are sure the
        # earliest has passed as well.
        check_state_val = rps._task_state_values[rps.FINAL[-1]]
        for state in states:
            check_state_val = min(check_state_val,
                                  rps._task_state_values[state])

        start    = time.time()
        to_check = None

        with self._tasks_lock:
            to_check = [self._tasks[uid] for uid in uids]

        # We don't want to iterate over all tasks again and again, as that would
        # duplicate checks on tasks which were found in matching states.  So we
        # create a list from which we drop the tasks as we find them in
        # a matching state
        self._rep.progress_tgt(len(to_check), label='wait')
        self._log.debug('WAIT for %d tasks', len(to_check))
        while to_check and not self._terminate.is_set():

          # self._log.debug('wait for %d tasks', len(to_check))

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
                  # self._log.debug('wait again for %s [%s]', task.uid, task.state)
                    check_again.append(task)

                else:
                    # stop watching this task
                  # self._log.debug('wait ok    for %s [%s]', task.uid, task.state)
                    if task.state in [rps.FAILED]:
                        self._rep.progress()  # (color='error', c='-')
                    elif task.state in [rps.CANCELED]:
                        self._rep.progress()  # (color='warn', c='*')
                    else:
                        self._rep.progress()  # (color='ok', c='+')

            to_check = check_again

        self._log.debug('wait completed')

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
    def cancel_tasks(self, uids=None):
        """Cancel one or more :class:`radical.pilot.Task` s.

        Note that cancellation of tasks is not immediate, i.e. their state is
        not necessarily `CANCELED` after this method returns.  Instead, the
        cancellation request is sent to the components which currently manage
        the tasks and which then will enact the request at their discretion,
        eventually leading to the state transition to `CANCELLED`.

        Arguments:
            uids (str | list[str]): The IDs of the tasks to cancel.
        """

        if not uids:
            with self._tasks_lock:
                uids  = list(self._tasks.keys())
        else:
            if not isinstance(uids, list):
                uids = [uids]

        # we *always* issue the cancellation command to the local components
        self.publish(rpc.CONTROL_PUBSUB, {'cmd' : 'cancel_tasks',
                                          'arg' : {'uids' : uids,
                                                   'tmgr' : self.uid},
                                          'fwd' : True})

        # We do not wait and block the call until all the tasks are marked
        # cancelled.  This means when inspecting for state just after a state
        # change, we may observe a old state, instead of CANCELLED.


    # --------------------------------------------------------------------------
    #
    # TODO: `metric` -> `metrics`, for consistency with `unregister_callback()`
    #
    def register_callback(self, cb, cb_data=None, metric=None, uid=None):
        """Registers a new callback function with the TaskManager.

        Manager-level
        callbacks get called if the specified metric changes.  The default
        metric `TASK_STATE` fires the callback if any of the Tasks
        managed by the PilotManager change their state.

        All callback functions need to have the same signature::

            def cb(obj, value) -> None:
                ...

        where ``obj`` is a handle to the object that triggered the callback,
        ``value`` is the metric, and ``data`` is the data provided on
        callback registration..  In the example of `TASK_STATE` above, the
        object would be the task in question, and the value would be the new
        state of the task.

        If ``cb_data`` is given, then the ``cb`` signature changes to
        ::

            def cb(obj, state, cb_data) -> None:
                ...

        and ``cb_data`` are passed unchanged.

        If ``uid`` is given, the callback will invoked only for the specified
        task.

        Available metrics are

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

        metrics = ru.as_list(metric)

        if not uid:
            uid = '*'

        elif uid not in self._tasks:
            raise ValueError('no such task %s' % uid)


        with self._tcb_lock:

            for metric in metrics:

                if metric not in rpc.TMGR_METRICS:
                    raise ValueError("invalid tmgr metric '%s'" % metric)

                cb_id = id(cb)

                if metric not in self._callbacks:
                    self._callbacks[metric] = dict()

                if uid not in self._callbacks[metric]:
                    self._callbacks[metric][uid] = dict()

                self._callbacks[metric][uid][cb_id] = {'cb'     : cb,
                                                       'cb_data': cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb=None, metrics=None, uid=None):

        if not metrics:
            metrics = rpc.TASK_STATE

        metrics = ru.as_list(metrics)

        if not uid:
            uid = '*'

        elif uid not in self._tasks:
            raise ValueError('no such task %s' % uid)

        for metric in metrics:
            if metric not in rpc.TMGR_METRICS:
                raise ValueError("invalid tmgr metric '%s'" % metric)

        with self._tcb_lock:

            for metric in metrics:

                if metric not in self._callbacks:
                    raise ValueError("cb metric '%s' invalid" % metric)

                if uid not in self._callbacks[metric]:
                    raise ValueError("cb target '%s' invalid" % uid)

                if cb:
                    to_delete = [id(cb)]
                else:
                    to_delete = list(self._callbacks[metric][uid].keys())

                for cb_id in to_delete:

                    if cb_id not in self._callbacks[metric][uid]:
                        raise ValueError("cb %s not registered" % cb_id)

                    del self._callbacks[metric][uid][cb_id]


# ------------------------------------------------------------------------------
