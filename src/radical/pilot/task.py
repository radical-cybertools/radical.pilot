
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import copy
import time

import threading     as mt

import radical.utils as ru

from . import states    as rps
from . import utils     as rpu
from . import constants as rpc

from .staging_directives import expand_description
from .task_description   import TaskDescription, TASK_SERVICE
from .resource_config    import Slot


# ------------------------------------------------------------------------------
#
class Task(object):
    """Represent a 'task' that is executed on a Pilot.

    Tasks allow to control and query the state of this task.

    Note:
        A task cannot be created directly. The factory method
        :func:`rp.TaskManager.submit_tasks` has to be used instead.

    Example::

        tmgr = rp.TaskManager(session=s)

        ud = rp.TaskDescription()
        ud.executable = "/bin/date"

        task = tmgr.submit_tasks(ud)

    """
    # --------------------------------------------------------------------------
    # In terms of implementation, a Task is not much more than a dict whose
    # content are dynamically updated to reflect the state progression through
    # the TMGR components.  As a Task is always created via a TMGR, it is
    # considered to *belong* to that TMGR, and all activities are actually
    # implemented by that TMGR.
    #
    # Note that this implies that we could create CUs before submitting them
    # to a TMGR, w/o any problems. (FIXME?)
    # --------------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #
    def __init__(self, tmgr, descr, origin):

        # ensure that the description is viable
        descr.verify()

        # 'static' members
        self._tmgr   = tmgr
        self._descr  = descr
        self._origin = origin

        # initialize state
        self._session          = self._tmgr.session
        self._uid              = self._descr.uid
        self._state            = rps.NEW
        self._log              = tmgr._log
        self._exit_code        = None
        self._stdout           = str()
        self._stderr           = str()
        self._ofiles           = None
        self._return_value     = None
        self._exception        = None
        self._exception_detail = None
        self._info             = None
        self._info_evt         = mt.Event()
        self._pilot            = self._descr.pilot
        self._endpoint_fs      = None
        self._resource_sandbox = None
        self._session_sandbox  = None
        self._pilot_sandbox    = None
        self._task_sandbox     = None
        self._client_sandbox   = None
        self._callbacks        = dict()
        self._slots            = None
        self._partition        = None

        # ensure uid
        if not self._uid:
            raise ValueError('task uid must be specified in task description')

        for m in rpc.TMGR_METRICS:
            self._callbacks[m] = dict()

        # we always invke the default state cb
        self._callbacks[rpc.TASK_STATE][self._default_state_cb.__name__] = {
                'cb'      : self._default_state_cb,
                'cb_data' : None}

        # If staging directives exist, expand them to the full dict version.  Do
        # not, however, expand any URLs as of yet, as we likely don't have
        # sufficient information about pilot sandboxes etc.
        expand_description(self._descr)

        self._tmgr.advance(self.as_dict(), rps.NEW, publish=False, push=False)


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str([self.uid, self.pilot, self.state])


    # --------------------------------------------------------------------------
    #
    def _default_state_cb(self, task, state=None):

        self._log.info("[Callback]: task %s state: %s.", self.uid, self.state)


    # --------------------------------------------------------------------------
    #
    def _update(self, task_dict, reconnect=False):
        """State change updater.

        This will update the facade object after state changes etc, and is
        invoked by whatever component receiving that updated information.
        """

        assert task_dict['uid'] == self.uid, 'update called on wrong instance'

        # this method relies on state updates to arrive in order
        current = self.state
        target  = task_dict['state']

        # we never update once the task is FAILED or DONE
        if current in [rps.FAILED, rps.DONE]:
            self._log.debug('task %s is final, ignore update', self.uid)
            return

        # when in CANCELED state, we only allow updates for `DONE` tasks - in
        # that case the cancel command raced the task execution, and the
        # execution actually won, so we don't want to waste that work
        if current == rps.CANCELED and target != rps.DONE:
            self._log.debug('task %s was CANCELED, state not updated', self.uid)
            target = current


        if not reconnect:
            if target not in [rps.FAILED, rps.CANCELED]:
                s_tgt = rps._task_state_value(target)
                s_cur = rps._task_state_value(current)
                if s_tgt - s_cur != 1:
                    self._log.error('%s: invalid state transition %s -> %s',
                                    self.uid, current, target)
                    raise RuntimeError('invalid state transition %s: %s -> %s'
                            % (self.uid, current, target))

        # we update all fields
        # FIXME: well, not all really :/
        # FIXME: setattr is ugly...  we should maintain all state in a dict.
        for key in ['state', 'stdout', 'stderr', 'exit_code', 'return_value',
                    'endpoint_fs', 'resource_sandbox', 'session_sandbox',
                    'pilot', 'pilot_sandbox', 'task_sandbox', 'client_sandbox',
                    'exception', 'exception_detail', 'slots', 'partition',
                    'ofiles']:

            val = task_dict.get(key, None)
            if val is not None:
                setattr(self, "_%s" % key, val)

        metadata = task_dict.get('description', {}).get('metadata')
        if metadata:
            self._descr['metadata'] = metadata

        # if a service is finalized, set info_wait event (only # once)
        if target in rps.FINAL and current not in rps.FINAL:
            if self._descr.mode == TASK_SERVICE:
                # signal failure in case we are still waiting for the service
                self._set_info(None)

        # callbacks are not invoked here, but are bulked in the tmgr


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object."""

        ret = {
            'type':             'task',
            'tmgr':             self.tmgr.uid,
            'uid':              self.uid,
            'name':             self.name,
            'state':            self.state,
            'origin':           self.origin,
            'exit_code':        self.exit_code,
            'stdout':           self.stdout,
            'stderr':           self.stderr,
            'return_value':     self.return_value,
            'exception':        self.exception,
            'exception_detail': self.exception_detail,
            'pilot':            self.pilot,
            'endpoint_fs':      self.endpoint_fs,
            'resource_sandbox': self.resource_sandbox,
            'session_sandbox':  self.session_sandbox,
            'pilot_sandbox':    self.pilot_sandbox,
            'task_sandbox':     self.task_sandbox,
            'client_sandbox':   self.client_sandbox,
            'info':             self.info,
            'slots':            self.slots,
            'partition':        self.partition,
            'description':      self.description,   # this is a deep copy
        }

        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        """radical.pilot.Session: The task's session."""
        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def tmgr(self):
        """radical.pilot.TaskManager: The task's manager."""
        return self._tmgr


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """str: The task's unique identifier within a :class:`TaskManager`."""
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        """str: The task's application specified name."""
        return self._descr.name


    # --------------------------------------------------------------------------
    #
    @property
    def mode(self):
        """str: The task mode."""
        return self._descr.mode


    # --------------------------------------------------------------------------
    #
    @property
    def origin(self):
        """str: Indicates where the task was created."""
        return self._origin


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """str: The current state of the task."""
        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        """int: The exit code of the task, if that is already known, or
        'None' otherwise.
        """
        return self._exit_code


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """str: A snapshot of the executable's STDOUT stream.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will return None.

        Warning:
            This can be inefficient.  Output may be incomplete and/or filtered.

        """
        return self._stdout


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """str: A snapshot of the executable's STDERR stream.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will return None.

        Warning:
            This can be inefficient.  Output may be incomplete and/or filtered.

        """
        return self._stderr


    # --------------------------------------------------------------------------
    #
    @property
    def output_files(self):
        """list[str]: A list of output file names.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will return None.

        Warning:
            This can be incomplete: the heuristics will not detect files which
            start with `<task_id>.`, for example.  It will also not detect files
            which are not created in the task sandbox.

        """
        return self._ofiles


    # --------------------------------------------------------------------------
    #
    @property
    def return_value(self):
        """Any: The return value for tasks which represent function call (or
        None otherwise).

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will always return None.
        """
        return self._return_value


    # --------------------------------------------------------------------------
    #
    @property
    def exception(self):
        """str: A string representation (`__repr__`) of the exception which
        caused the task's `FAILED` state if such one was raised while managing
        or executing the task.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will always return None.
        """
        return self._exception


    # --------------------------------------------------------------------------
    #
    @property
    def exception_detail(self):
        """str: Additional information about the exception which caused this
        task to enter FAILED state.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will always return None.
        """
        return self._exception_detail


    # --------------------------------------------------------------------------
    #
    @property
    def pilot(self):
        """str: The pilot ID of this task, if that is already known, or
        'None' otherwise.
        """
        return self._pilot


    # --------------------------------------------------------------------------
    #
    @property
    def sandbox(self):
        """str: An alias for :attr:`~radical.pilot.Task.task_sandbox`."""
        return self.task_sandbox


    @property
    def task_sandbox(self):
        """radical.utils.Url: The full sandbox URL of this task, if that is already
        known, or 'None' otherwise.
        """
        return self._task_sandbox


    @property
    def endpoint_fs(self):
        """radical.utils.Url: The URL which is internally used to access the
        target resource's root file system.
        """
        return self._endpoint_fs


    @property
    def resource_sandbox(self):
        """radical.utils.Url: The full URL of the path that RP considers the
        resource sandbox, i.e., the sandbox on the target resource's file system
        that is shared by all sessions which access that resource.
        """
        return self._resource_sandbox


    @property
    def session_sandbox(self):
        """radical.utils.Url: The full URL of the path that RP considers the
        session sandbox on the target resource's file system which is shared
        by all pilots which access that resource in the current session.
        """
        return self._session_sandbox


    @property
    def pilot_sandbox(self):
        """radical.utils.Url: The full URL of the path that RP considers the
        pilot sandbox on the target resource's file system which is shared
        by all tasks which are executed by that pilot.
        """
        return self._pilot_sandbox


    @property
    def client_sandbox(self):
        """radical.utils.Url: The full URL of the client sandbox, which is
        usually the same as the current working directory of the Python script
        in which the RP Session is instantiated.

        Note that the URL may not be usable to access
        that sandbox from another machine: RP in general knows nothing about
        available access endpoints on the local host.
        """
        return self._client_sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        """dict: The description the task was started with, as a dictionary."""
        return self._descr


    # --------------------------------------------------------------------------
    #
    @property
    def slots(self):
        '''dict: The slots assigned for the task's execution'''
        if self._slots:
            if not self._slots[0].get('version'):
                for idx,slot in enumerate(self._slots):
                    self._slots[idx] = Slot(self._slots[idx])
        return self._slots


    # --------------------------------------------------------------------------
    #
    @property
    def partition(self):
        '''dict: The pilot partition assigned for the task's execution'''
        return self._partition


    # --------------------------------------------------------------------------
    #
    @property
    def metadata(self):
        """The metadata field of the task's description."""
        return self._descr.metadata


    # --------------------------------------------------------------------------
    #
    @property
    def info(self):
        """The metadata field of the task's description."""
        return self._info

    def _set_info(self, info):
        self._info = info
        self._info_evt.set()

    def wait_info(self, timeout=None):

        self._info_evt.wait(timeout=timeout)

        if self._info is None:
            raise RuntimeError('no service info: %s / %s'
                              % (self.stdout, self.stderr))
        return self.info


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, cb_data=None, metric=None):
        """Add a state-change callback.

        Registers a callback function that is triggered every time a
        task's state changes.

        All callback functions need to have the same signature::

            def cb(obj, state) -> None:
                ...

        where ``obj`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If ``cb_data`` is given,
        then the ``cb`` signature changes to
        ::

            def cb(obj, state, cb_data) -> None:
                ...

        and ``cb_data`` are passed unchanged.
        """

        if not metric:
            metric = rpc.TASK_STATE

        self._tmgr.register_callback(cb, cb_data, metric=metric, uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        """Block for state change.

        Returns when the task reaches a specific state or
        when an optional timeout is reached.

        Arguments:
            state (str | list[str], optional): The state(s) that task has to reach
                in order for the call to return.

                By default `wait` waits for the task to reach a **final**
                state, which can be one of the following.

                * :data:`rp.states.DONE`
                * :data:`rp.states.FAILED`
                * :data:`rp.states.CANCELED`
            timeout (float, optional): Optional timeout in seconds before the
                call returns regardless whether the task has reached the desired
                state or not.  The default value **None** never times out.

        """

        if not state:
            states = rps.FINAL
        if not isinstance(state, list):
            states = [state]
        else:
            states = state


        if self.state in rps.FINAL:
            # we will never see another state progression.  Raise an error
            # (unless we waited for this)
            if self.state in states:
                return self.state

            # FIXME: do we want a raise here, really?  This introduces a race,
            #        really, on application level
            # raise RuntimeError("can't wait on a task in final state")
            return self.state

        start_wait = time.time()
        while self.state not in states:

            time.sleep(0.1)

            if timeout and (timeout <= (time.time() - start_wait)):
                break

            if self._tmgr._terminate.is_set():
                break

        return self.state


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """Cancel the task."""

        self._tmgr.cancel_tasks(self.uid)


# ------------------------------------------------------------------------------
#
class TaskDict(rpu.FastTypedDict):
    """Dictionary encoded Task.

    rp.Task is an API level object and as that is not a useful internal
    representation of an task on the level of RP components and message
    channels.  Instead, a task is there represented as a dictionary.  To
    facilitate a minimum of documentation and type consistency, this class
    defines such task dictionaries as `TypedDict` objects.
    """

    _schema = {
            # where the task got created
            # CLIENT | AGENT
            'origin'      : str,

            # original task description used to create the task
            'description' : TaskDescription,

          # # what scope should be notified for task state changes
          # # CLIENT, AGENT, RAPTOR - can be empty
          # 'notification': [str],

          # # what exact resources were used to execute the task
          # # [[node_uid:core_id:gpu_id, ...]]
          # 'resources'   : [None],
    }

    _defaults = {
            'origin'      : None,
            'description' : None,
          # 'notification': [],
          # 'resources'   : [],
    }


# ------------------------------------------------------------------------------
