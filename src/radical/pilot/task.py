
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import copy
import time

import radical.utils as ru

from . import states    as rps
from . import constants as rpc

from .staging_directives import expand_description
from .task_description   import TaskDescription


_uids = list()


# ------------------------------------------------------------------------------
#
def _check_uid(uid):
    # ensure that uid is not yet known

    if uid in _uids:
        return False
    else:
        _uids.append(uid)
        return True


# ------------------------------------------------------------------------------
#
class Task(object):
    '''
    A Task represent a 'task' that is executed on a Pilot.
    Tasks allow to control and query the state of this task.

    .. note:: A task cannot be created directly. The factory method
              :meth:`rp.TaskManager.submit_tasks` has to be used instead.

                **Example**::

                      tmgr = rp.TaskManager(session=s)

                      ud = rp.TaskDescription()
                      ud.executable = "/bin/date"

                      task = tmgr.submit_tasks(ud)
    '''
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
        self._descr  = descr.as_dict()
        self._origin = origin

        # initialize state
        self._session          = self._tmgr.session
        self._uid              = self._descr.get('uid')
        self._state            = rps.NEW
        self._log              = tmgr._log
        self._exit_code        = None
        self._stdout           = None
        self._stderr           = None
        self._return_value     = None
        self._exception        = None
        self._exception_detail = None
        self._pilot            = descr.get('pilot')
        self._endpoint_fs      = None
        self._resource_sandbox = None
        self._session_sandbox  = None
        self._pilot_sandbox    = None
        self._task_sandbox     = None
        self._client_sandbox   = None
        self._callbacks        = dict()

        # ensure uid is unique
        if self._uid:
            if not _check_uid(self._uid):
                raise ValueError('uid %s is not unique' % self._uid)
        else:
            self._uid = ru.generate_id('task.%(item_counter)06d', ru.ID_CUSTOM,
                                       ns=self._session.uid)

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
    def __repr__(self):

        return '<%s object, uid %s>' % (self.__class__.__qualname__, self._uid)


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
        '''
        This will update the facade object after state changes etc, and is
        invoked by whatever component receiving that updated information.
        '''

        assert task_dict['uid'] == self.uid, 'update called on wrong instance'

        # this method relies on state updates to arrive in order
        current = self.state
        target  = task_dict['state']

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
                    'exception', 'exception_detail']:

            val = task_dict.get(key, None)
            if val is not None:
                setattr(self, "_%s" % key, val)

        # callbacks are not invoked here anymore, but are bulked in the tmgr


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        '''
        Returns a Python dictionary representation of the object.
        '''

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
            'description':      self.description   # this is a deep copy
        }

        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        '''
        Returns the task's session.

        **Returns:**
            * A :class:`Session`.
        '''

        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def tmgr(self):
        '''
        Returns the task's manager.

        **Returns:**
            * A :class:`TaskManager`.
        '''

        return self._tmgr


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        '''
        Returns the task's unique identifier.

        The uid identifies the task within a :class:`TaskManager`.

        **Returns:**
            * A unique identifier (string).
        '''
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        '''
        Returns the task's application specified name.

        **Returns:**
            * A name (string).
        '''
        return self._descr.get('name')


    # --------------------------------------------------------------------------
    #
    @property
    def mode(self):
        '''
        Returns the task mode

        **Returns:**
            * A mode (string).
        '''
        return self._descr['mode']


    # --------------------------------------------------------------------------
    #
    @property
    def origin(self):
        '''
        indicates where the task was created

        **Returns:**
            * string
        '''

        return self._origin


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        '''
        Returns the current state of the task.

        **Returns:**
            * state (string enum)
        '''

        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        '''
        Returns the exit code of the task, if that is already known, or
        'None' otherwise.

        **Returns:**
            * exit code (int)
        '''

        return self._exit_code


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        '''
        Returns a snapshot of the executable's STDOUT stream.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stdout (string)
        '''

        return self._stdout


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        '''
        Returns a snapshot of the executable's STDERR stream.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stderr (string)
        '''

        return self._stderr


    # --------------------------------------------------------------------------
    #
    @property
    def return_value(self):
        '''
        Returns the return value for tasks which represent function call (or
        None otherwise).

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will always return None.

        **Returns:**
            * Any
        '''

        return self._return_value


    # --------------------------------------------------------------------------
    #
    @property
    def exception(self):
        '''
        Returns an string representation (`__repr__`) of the exception which
        caused the task's `FAILED` state if such one was raised while managing
        or executing the task.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will always return None.

        **Returns:**
            * str
        '''

        return self._exception


    # --------------------------------------------------------------------------
    #
    @property
    def exception_detail(self):
        '''
        Returns additional information about the exception which caused this
        task to enter FAILED state.

        If this property is queried before the task has reached
        'DONE' or 'FAILED' state it will always return None.

        **Returns:**
            * str
        '''

        return self._exception_detail


    # --------------------------------------------------------------------------
    #
    @property
    def pilot(self):
        '''
        Returns the pilot ID of this task, if that is already known, or
        'None' otherwise.

        **Returns:**
            * A pilot ID (string)
        '''

        return self._pilot


    # --------------------------------------------------------------------------
    #
    @property
    def working_directory(self):         # **NOTE:** deprecated, use *`sandbox`*
        return self.sandbox


    @property
    def sandbox(self):
        return self.task_sandbox


    @property
    def task_sandbox(self):
        '''
        Returns the full sandbox URL of this task, if that is already
        known, or 'None' otherwise.

        **Returns:**
            * A URL (radical.utils.Url).
        '''

        # NOTE: The task has a sandbox property, containing the full sandbox
        #       path, which is used by the tmgr to stage data back and forth.
        #       However, the full path as visible from the tmgr side might not
        #       be what the agent is seeing, specifically in the case of
        #       non-shared filesystems (OSG).  The agent thus uses
        #       `$PWD/t['uid']` as sandbox, with the assumption that this will
        #       get mapped to whatever is here returned as sandbox URL.
        #
        #       There is thus implicit knowledge shared between the RP client
        #       and the RP agent on how the sandbox path is formed!

        return self._task_sandbox


    @property
    def endpoint_fs(self):
        return self._endpoint_fs

    @property
    def resource_sandbox(self):
        return self._resource_sandbox

    @property
    def session_sandbox(self):
        return self._session_sandbox

    @property
    def pilot_sandbox(self):
        return self._pilot_sandbox

    @property
    def client_sandbox(self):
        return self._client_sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        '''
        Returns the description the task was started with, as a dictionary.

        **Returns:**
            * description (dict)
        '''

        return copy.deepcopy(self._descr)


    # --------------------------------------------------------------------------
    #
    @property
    def metadata(self):
        '''
        Returns the metadata field of the task's description
        '''

        return copy.deepcopy(self._descr.get('metadata'))


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, cb_data=None, metric=None):
        '''
        Registers a callback function that is triggered every time a
        task's state changes.

        All callback functions need to have the same signature::

            def cb(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If 'cb_data' is given,
        then the 'cb' signature changes to

            def cb(obj, state, cb_data)

        and 'cb_data' are passed unchanged.
        '''

        if not metric:
            metric = rpc.TASK_STATE

        self._tmgr.register_callback(cb, cb_data, metric=metric, uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        '''
        Returns when the task reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that task has to reach in order for the
              call to return.

              By default `wait` waits for the task to reach a **final**
              state, which can be one of the following:

              * :data:`rp.states.DONE`
              * :data:`rp.states.FAILED`
              * :data:`rp.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the task has reached the desired state or not.  The
              default value **None** never times out.  '''

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
                return

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
        '''
        Cancel the task.
        '''

        self._tmgr.cancel_tasks(self.uid)


# ------------------------------------------------------------------------------
#
class TaskDict(ru.TypedDict):
    '''
    rp.Task is an API level object and as that is not a useful internal
    representation of an task on the level of RP components and message
    channels.  Instead, a task is there represented as a dictionary.  To
    facilitate a minimum of documentation and type consistency, this class
    defines such task dictionaries as `TypedDict` objects.
    '''

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

