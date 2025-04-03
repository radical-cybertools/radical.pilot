# pylint: disable=protected-access

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import copy
import time

import threading     as mt

import radical.utils as ru

from . import PilotManager
from . import states    as rps
from . import constants as rpc

from .messages             import RPCRequestMessage, RPCResultMessage
from .staging_directives   import complete_url
from .resource_config      import Node, NodeList, NumaNode


# ------------------------------------------------------------------------------
#
class Pilot(object):
    """Represent a resource overlay on a local or remote resource.

    Note:
        A Pilot cannot be created directly. The factory method
        :func:`radical.pilot.PilotManager.submit_pilots` has to be
        used instead.

    Example::

          pm = radical.pilot.PilotManager(session=s)
          pd = radical.pilot.PilotDescription()

          pd.resource = "local.localhost"
          pd.cores    = 2
          pd.runtime  = 5 # minutes

          pilot = pm.submit_pilots(pd)

    """

    # --------------------------------------------------------------------------
    # In terms of implementation, a Pilot is not much more than a dict whose
    # content are dynamically updated to reflect the state progression through
    # the PMGR components.  As a Pilot is always created via a PMGR, it is
    # considered to *belong* to that PMGR, and all activities are actually
    # implemented by that PMGR.
    #
    # Note that this implies that we could create Pilots before submitting them
    # to a PMGR, w/o any problems. (FIXME?)
    # --------------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #
    def __init__(self, pmgr: PilotManager, descr):

        # sanity checks on description
        if not descr.runtime:
            raise ValueError('pilot runtime must be defined')

        if descr.runtime <= 0:
            raise ValueError('pilot runtime must be positive')

        if not descr.resource:
            raise ValueError('pilot target resource must be defined')


        # initialize state
        self._descr      = descr.as_dict()
        self._pmgr       = pmgr
        self._session    = self._pmgr.session
        self._prof       = self._session._prof
        self._uid        = self._descr.get('uid')
        self._state      = rps.NEW
        self._log        = pmgr._log
        self._sub        = None
        self._pilot_dict = dict()
        self._callbacks  = dict()
        self._cb_lock    = ru.RLock()
        self._tmgr       = None
        self._nodelist   = None

        # pilot failures can trigger app termination
        self._exit_on_error = self._descr.get('exit_on_error')

        # ensure uid is unique
        if self._uid:
            if not self._pmgr.check_uid(self._uid):
                raise ValueError('uid %s is not unique' % self._uid)
        else:
            self._uid = ru.generate_id('pilot.%(item_counter)04d', ru.ID_CUSTOM,
                                       ns=self._session.uid)

        for m in rpc.PMGR_METRICS:
            self._callbacks[m] = dict()

        # we always invoke the default state cb
        self._callbacks[rpc.PILOT_STATE][self._default_state_cb.__name__] = {
                'cb'      : self._default_state_cb,
                'cb_data' : None}

        # `as_dict()` needs `pilot_dict` and other attributes.  Those should all
        # be available at this point (apart from the sandboxes), so we now
        # query for those sandboxes.
        self._pilot_jsurl      = ru.Url()
        self._pilot_jshop      = ru.Url()
        self._endpoint_fs      = ru.Url()
        self._resource_sandbox = ru.Url()
        self._session_sandbox  = ru.Url()
        self._pilot_sandbox    = ru.Url()
        self._client_sandbox   = ru.Url()

        pilot = self.as_dict()

        self._pilot_jsurl, self._pilot_jshop \
                               = self._session._get_jsurl           (pilot)
        self._endpoint_fs      = self._session._get_endpoint_fs     (pilot)
        self._resource_sandbox = self._session._get_resource_sandbox(pilot)
        self._session_sandbox  = self._session._get_session_sandbox (pilot)
        self._pilot_sandbox    = self._session._get_pilot_sandbox   (pilot)
        self._client_sandbox   = self._session._get_client_sandbox()

        # contexts for staging url expansion
        # NOTE: no task sandboxes defined!
        self._rem_ctx = {'pwd'     : self._pilot_sandbox,
                         'client'  : self._client_sandbox,
                         'pilot'   : self._pilot_sandbox,
                         'resource': self._resource_sandbox,
                         'session' : self._session_sandbox,
                         'endpoint': self._endpoint_fs}

        self._loc_ctx = {'pwd'     : self._client_sandbox,
                         'client'  : self._client_sandbox,
                         'pilot'   : self._pilot_sandbox,
                         'resource': self._resource_sandbox,
                         'session' : self._session_sandbox,
                         'endpoint': self._endpoint_fs}


        # we need to expand plaaceholders in the sandboxes
        # FIXME: this code is a duplication from the pilot launcher code
        expand = dict()
        for k,v in pilot['description'].items():
            if v is None:
                v = ''
            expand['pd.%s' % k] = v
            if isinstance(v, str):
                expand['pd.%s' % k.upper()] = v.upper()
                expand['pd.%s' % k.lower()] = v.lower()
            else:
                expand['pd.%s' % k.upper()] = v
                expand['pd.%s' % k.lower()] = v

        self._endpoint_fs     .path  = self._endpoint_fs     .path % expand
        self._resource_sandbox.path  = self._resource_sandbox.path % expand
        self._session_sandbox .path  = self._session_sandbox .path % expand
        self._pilot_sandbox   .path  = self._pilot_sandbox   .path % expand

        # hook into the control pubsub for rpc handling
        self._rpc_reqs = dict()
        ctrl_addr_sub  = self._session._reg['bridges.control_pubsub.addr_sub']
        ctrl_addr_pub  = self._session._reg['bridges.control_pubsub.addr_pub']

        self._ctrl_pub = ru.zmq.Publisher(rpc.CONTROL_PUBSUB, url=ctrl_addr_pub,
                                          log=self._log, prof=self._prof)

        self._sub = ru.zmq.Subscriber(rpc.CONTROL_PUBSUB, url=ctrl_addr_sub,
                                      log=self._log, prof=self._prof,
                                      cb=self._control_cb,
                                      topic=rpc.CONTROL_PUBSUB)


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str([self.uid, self.resource, self.state])


    # --------------------------------------------------------------------------
    #
    def _default_state_cb(self, pilot, state=None):

        uid   = self.uid
        state = self.state

        self._log.info("[Callback]: pilot %s state: %s.", uid, state)

        if state == rps.FAILED and self._exit_on_error:

            self._log.error("[Callback]: pilot '%s' failed (exit)", uid)
            self._sub.stop()

            # There are different ways to tell main...
          # ru.print_stacktrace()
            sys.stderr.write('=== pilot failed, exit_on_error ===\n')
            ru.cancel_main_thread('term')
          # raise RuntimeError('pilot %s failed - fatal!' % self.uid)
          # os.kill(os.getpid(), signal.SIGTERM)


    # --------------------------------------------------------------------------
    #
    def _update(self, pilot_dict):
        """Trigger an update.

        This will update the facade object after state changes etc, and is
        invoked by whatever component receiving that updated information.

        Returns:
             booL: True if state changed, False otherwise.

        """

        self._log.debug('update %s', pilot_dict['uid'])

        if pilot_dict['uid'] != self.uid:
            self._log.error('invalid uid: %s / %s', pilot_dict['uid'], self.uid)

        assert pilot_dict['uid'] == self.uid, 'update called on wrong instance'

        # NOTE: this method relies on state updates to arrive in order and
        #       without gaps.
        current = self.state
        target  = pilot_dict.get('state', self.state)

        if target not in [rps.FAILED, rps.CANCELED]:

            # ensure valid state transition
            state_diff = rps._pilot_state_value(target) - \
                         rps._pilot_state_value(current)
            if state_diff > 1:
                raise RuntimeError('%s: invalid state transition %s -> %s',
                                   self.uid, current, target)

        self._state = target

        if self._state in rps.FINAL:
            self._sub.stop()

        # FIXME: this is a hack to get the resource details into the pilot
        resources = pilot_dict.get('resources') or {}
        rm_info   = resources.get('rm_info')

        if rm_info:
            del pilot_dict['resources']['rm_info']
            pilot_dict['resource_details'] = rm_info

        for k in list(pilot_dict.keys()):
            if pilot_dict[k] is None:
                del pilot_dict[k]

        # keep all other information around
        ru.dict_merge(self._pilot_dict, pilot_dict, ru.OVERWRITE)

        # invoke pilot specific callbacks
        # FIXME: this iteration needs to be thread-locked!
        with self._cb_lock:
            for _,cb_val in self._callbacks[rpc.PILOT_STATE].items():

                cb      = cb_val['cb']
                cb_data = cb_val['cb_data']

                self._log.debug('call %s', cb)

                self._log.debug('%s calls cb %s', self.uid, cb)

                if cb_data: cb([self], cb_data)
                else      : cb([self])

            # ask pmgr to invoke any global callbacks
            self._pmgr._call_pilot_callbacks(self)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Dictionary representation.

        Returns:
             dict: a Python dictionary representation of the object.

        """

        ret = {'session'          : self.session.uid,
               'pmgr'             : self.pmgr.uid,
               'uid'              : self.uid,
               'type'             : 'pilot',
               'state'            : self.state,
               'log'              : self.log,
               'stdout'           : self.stdout,
               'stderr'           : self.stderr,
               'resource'         : self.resource,
               'resources'        : self.resources,
               'endpoint_fs'      : str(self._endpoint_fs),
               'resource_sandbox' : str(self._resource_sandbox),
               'session_sandbox'  : str(self._session_sandbox),
               'pilot_sandbox'    : str(self._pilot_sandbox),
               'client_sandbox'   : str(self._client_sandbox),
               'js_url'           : str(self._pilot_jsurl),
               'js_hop'           : str(self._pilot_jshop),
               'description'      : self.description,  # this is a deep copy
               'resource_details' : self.resource_details,
               'nodelist'         : self.nodelist,
              }

        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        """Session: The pilot's session."""

        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def pmgr(self):
        """PilotManager: The pilot's manager."""

        return self._pmgr


    # -------------------------------------------------------------------------
    #
    @property
    def resource_details(self):
        """dict: agent level resource information."""

        return self._pilot_dict.get('resource_details')


    # -------------------------------------------------------------------------
    #
    @property
    def nodelist(self):
        '''NodeList, describing the nodes the pilot can place tasks on'''

        if not self._nodelist:

            resource_details = self.resource_details
            if not resource_details:
                return None

            numa_domain_map = resource_details.get('numa_domain_map')
            node_list       = resource_details.get('node_list')

            if not node_list:
                return None

            # only create NUMA resources if a numa domain map is available
            if not numa_domain_map:
                nodes = [Node(node) for node in node_list]
            else:
                nodes = [NumaNode(node, numa_domain_map)
                                       for node in node_list]

            self._nodelist = NodeList(nodes=nodes)
            self._nodelist.verify()

        return self._nodelist


    # --------------------------------------------------------------------------
    #
    def rest_url(self):
        '''
        Returns the agent's rest URL (only in `ACTIVE` state or later)
        '''

        return self._pilot_dict.get('rest_url')


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """str: The pilot's unique identifier within a :class:`PilotManager`."""

        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """str: The current :py:mod:`state <radical.pilot.states>` of the pilot."""

        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def log(self):
        """list[tuple]: A list of human readable [timestamp, string] tuples describing
        various events during the pilot's lifetime.  Those strings are not
        normative, only informative!
        """

        return self._pilot_dict.get('log')


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """str: A snapshot of the pilot's STDOUT stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        Warning:
            This can be inefficient. Output may be incomplete and/or filtered.

        """

        return self._pilot_dict.get('stdout')


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """str: A snapshot of the pilot's STDERR stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        Warning:
            This can be inefficient.  Output may be incomplete and/or filtered.

        """

        return self._pilot_dict.get('stderr')


    # --------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """str: The resource tag of this pilot."""

        return self._descr.get('resource')


    # --------------------------------------------------------------------------
    #
    @property
    def resources(self):
        """str: The amount of resources used by this pilot."""

        return self._pilot_dict.get('resources')


    # --------------------------------------------------------------------------
    #
    @property
    def pilot_sandbox(self):
        """str: The full sandbox URL of this pilot, if that is already
        known, or 'None' otherwise.
        """

        # NOTE: The pilot has a sandbox property, containing the full sandbox
        #       path, which is used by the pmgr to stage data back and forth.
        #       However, the full path as visible from the pmgr side might not
        #       be what the agent is seeing, specifically in the case of
        #       non-shared filesystems (OSG).  The agent thus uses
        #       `$PWD` as sandbox, with the assumption that this will
        #       get mapped to whatever is here returned as sandbox URL.
        #
        #       There is thus implicit knowledge shared between the RP client
        #       and the RP agent that `$PWD` *is* the sandbox!  The same
        #       implicitly also holds for the staging area, which is relative
        #       to the pilot sandbox.
        if self._pilot_sandbox:
            return str(self._pilot_sandbox)


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
        which is shared by all sessions which access that resource.
        """
        return self._resource_sandbox


    @property
    def session_sandbox(self):
        """radical.utils.Url: The full URL of the path that RP considers the
        session sandbox on the target resource's file system which is shared by
        all pilots which access that resource in the current session.
        """
        return self._session_sandbox

    @property
    def client_sandbox(self):
        return self._client_sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        """dict: The description the pilot was started with, as a dictionary."""

        return copy.deepcopy(self._descr)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, metric=rpc.PILOT_STATE, cb_data=None):
        """Add callback for state changes.

        Registers a callback function that is triggered every time the
        pilot's state changes.

        All callback functions need to have the same signature::

            def cb(obj, state)

        where ``obj`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If *cb_data* is given,
        then the *cb* signature changes to
        ::

            def cb(obj, state, cb_data)

        and *cb_data* are passed along.

        """

        if metric not in rpc.PMGR_METRICS :
            raise ValueError ("invalid pmgr metric '%s'" % metric)

        with self._cb_lock:
            cb_id = id(cb)
            self._callbacks[metric][cb_id] = {'cb'      : cb,
                                              'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb, metric=rpc.PILOT_STATE):

        if metric and metric not in rpc.PMGR_METRICS :
            raise ValueError ("invalid pmgr metric '%s'" % metric)

        if not metric:
            metrics = rpc.PMGR_METRICS
        elif isinstance(metric, list):
            metrics =  metric
        else:
            metrics = [metric]

        with self._cb_lock:

            for metric in metrics:

                if cb:
                    to_delete = [id(cb)]
                else:
                    to_delete = list(self._callbacks[metric].keys())

                for cb_id in to_delete:

                    if cb_id not in self._callbacks[metric]:
                        raise ValueError("unknown callback '%s'" % cb_id)

                    del self._callbacks[metric][cb_id]


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        """Block for state change.

        Returns when the pilot reaches a specific state or
        when an optional timeout is reached.

        Arguments:
            state (list[str]):
                The :py:mod:`state(s) <radical.pilot.states>` that pilot has to reach in
                order for the call to return.

                By default `wait` waits for the pilot to reach a **final**
                state, which can be one of the following:

                * :data:`radical.pilot.states.DONE`
                * :data:`radical.pilot.states.FAILED`
                * :data:`radical.pilot.states.CANCELED`
            timeout (float):
                Optional timeout in seconds before the call returns regardless
                whether the pilot has reached the desired state or not.  The
                default value **None** never times out.

        """

        if   not state                  : states = rps.FINAL
        elif not isinstance(state, list): states = [state]
        else                            : states = state


        if self.state in rps.FINAL:

            # we will never see another state progression.  Raise an error
            # (unless we waited for this)
            if self.state in states:
                return

            # FIXME: do we want a raise here, really?  This introduces a race,
            #        really, on application level
            # raise RuntimeError("can't wait on a pilot in final state")
            return self.state

        start_wait = time.time()
        while self.state not in states:

            time.sleep(0.1)
            if timeout and (timeout <= (time.time() - start_wait)):
                break

            if self._pmgr._terminate.is_set():
                break

        return self.state


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """Cancel the pilot."""

        self._pmgr.cancel_pilots(self._uid)


    # --------------------------------------------------------------------------
    #
    def attach_tmgr(self, tmgr) -> None:

        if self._tmgr:
            raise RuntimeError('this pilot is already attached to %s'
                               % self._tmgr.uid)
        self._tmgr = tmgr

      # if self._task_waitpool:
      #     self._tmgr.submit_tasks(self._task_waitpool)
      #
      # if self._raptor_waitpool:
      #     self._tmgr.submit_tasks(self._raptor_waitpool)


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, descriptions):

        for descr in descriptions:
            descr.pilot = self.uid

        if not self._tmgr:

          # self._task_waitpool.append(descriptions)
          # return  # FIXME: cannot return tasks here

            raise RuntimeError('pilot is not attached to a task manager, yet')

        return self._tmgr.submit_tasks(descriptions)


    # --------------------------------------------------------------------------
    #
    def submit_raptors(self, descriptions):

        if not self._tmgr:

          # self._task_waitpool.append(descriptions)
          # return  # FIXME: cannot return tasks here

            raise RuntimeError('pilot is not attached to a task manager, yet')

        return self._tmgr.submit_raptors(descriptions, self.uid)


    # --------------------------------------------------------------------------
    #
    def register_service(self, uid, info):
        '''
        Instead of running a service task and injecting the obtained service
        info into other task environments, we can register an external service
        with the pilot, and have the pilot inject the service info the same way.
        '''

        self.rpc('register_service', uid=uid, info=info)


    # --------------------------------------------------------------------------
    #
    def prepare_env(self, env_name, env_spec):
        """Prepare a virtual environment.

        Request the preparation of a task or worker environment on the target
        resource.  This call will block until the env is created.

        Arguments:
            env_name (str): name of the environment to prepare.
            env_spec (dict): specification of the environment to prepare,
                like::

                    {'type'    : 'shell',
                     'pre_exec': ['export PATH=$PATH:/opt/bin']},

                    {'type'    : 'venv',
                     'version' : '3.8',
                     'pre_exec': ['module load python'],
                     'setup'   : ['radical.pilot==1.0', 'pandas']},

                    {'type'    : 'conda',
                     'version' : '3.8',
                     'setup'   : ['numpy']}

                    {'type'   : 'conda',
                     'version': '3.8',
                     'path'   : '/path/to/ve',
                     'setup'  : ['numpy']}

                where the *type* specifies the environment type, *version*
                specifies the Python version to deploy, and *setup* specifies
                how the environment is to be prepared.  If *path* is specified
                the env will be created at that path.  If *path* is not
                specified, RP will place the named env in the pilot sandbox
                (under :file:`env/named_env_{name}`). If a VE exists at that
                path, it will be used as is (an update is not performed).
                *pre_exec* commands are executed before env creation and setup
                are attempted.

        Note:
            The optional `version` specifier is only interpreted up to minor
            version, subminor and less are ignored.

        """

        self.rpc('prepare_env', env_name=env_name, env_spec=env_spec)


    # --------------------------------------------------------------------------
    #
    def stage_in(self, sds):
        """Stage files "in".

        Stages the content of the :py:mod:`~radical.pilot.staging_directives`
        to the pilot sandbox.

        Please note the documentation of
        :func:`radical.pilot.staging_directives.complete_url` for details on
        path and sandbox semantics.
        """

        sds = ru.as_list(sds)

        for sd in sds:
            sd['source'] = str(complete_url(sd['source'], self._loc_ctx, self._log))
            sd['target'] = str(complete_url(sd['target'], self._rem_ctx, self._log))

        # ask the pmgr to send the staging requests to the stager
        self._pmgr._pilot_staging_input(self.uid, sds)

        return [sd['target'] for sd in sds]


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg_data):

        # we only listen for RPCResponse messages

        try:
            msg = ru.zmq.Message.deserialize(msg_data)

            if isinstance(msg, RPCResultMessage):

                self._log.debug_4('handle rpc result %s', msg)

                if msg.uid in self._rpc_reqs:
                    self._rpc_reqs[msg.uid]['res'] = msg
                    self._rpc_reqs[msg.uid]['evt'].set()

        except:
            pass


    # --------------------------------------------------------------------------
    #
    def rpc(self, cmd, *args, rpc_addr=None, **kwargs):
        '''Remote procedure call.

        Send am RPC command and arguments to the pilot and wait for the
        response.  This is a synchronous operation at this point, and it is not
        thread safe to have multiple concurrent RPC calls.
        '''

        # RPC's can only be handled in `PMGR_ACTIVE` state
        # FIXME: RPCs will hang vorever if the pilot dies after sending the msg
        self.wait(rps.PMGR_ACTIVE)

        if rpc_addr is None:
            rpc_addr = self.uid

        rpc_id  = ru.generate_id('%s.rpc' % self._uid)
        rpc_req = RPCRequestMessage(uid=rpc_id, cmd=cmd, addr=rpc_addr,
                                    args=args, kwargs=kwargs)

        self._rpc_reqs[rpc_id] = {
                'req': rpc_req,
                'res': None,
                'evt': mt.Event(),
                'time': time.time(),
                }

        self._ctrl_pub.put(rpc.CONTROL_PUBSUB, rpc_req)

        while True:

            if not self._rpc_reqs[rpc_id]['evt'].wait(timeout=60):
                self._log.debug('still waiting for rpc request %s', rpc_id)
                continue

            rpc_res = self._rpc_reqs[rpc_id]['res']

            if rpc_res.exc:
                raise RuntimeError('rpc failed: %s' % rpc_res.exc)

            return rpc_res.val


    # --------------------------------------------------------------------------
    #
    def stage_out(self, sds=None):
        """Stage data "out".

        Fetches the content of the :py:mod:`~radical.pilot.staging_directives`
        from the pilot sandbox.

        Please note the documentation of
        :func:`radical.pilot.staging_directives.complete_url` for details on
        path and sandbox semantics.
        """

        sds = ru.as_list(sds)

        if not sds:
            sds = [{'source': 'pilot:///staging_output.tgz',
                    'target': 'client:///staging_output.tgz',
                    'action': rpc.TRANSFER}]

        for sd in sds:
            sd['source'] = str(complete_url(sd['source'], self._rem_ctx, self._log))
            sd['target'] = str(complete_url(sd['target'], self._loc_ctx, self._log))

        # ask the pmgr to send the staging reuests to the stager
        self._pmgr._pilot_staging_output(self.uid, sds)

        return [sd['target'] for sd in sds]


# ------------------------------------------------------------------------------
