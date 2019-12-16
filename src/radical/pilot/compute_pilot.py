
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import copy
import time

import radical.utils as ru

from . import states    as rps
from . import constants as rpc


# ------------------------------------------------------------------------------
#
class ComputePilot(object):
    '''
    A ComputePilot represent a resource overlay on a local or remote resource.

    .. note:: A ComputePilot cannot be created directly. The factory method
              :meth:`radical.pilot.PilotManager.submit_pilots` has to be
              used instead.

        **Example**::

              pm = radical.pilot.PilotManager(session=s)
              pd = radical.pilot.ComputePilotDescription()

              pd.resource = "local.localhost"
              pd.cores    = 2
              pd.runtime  = 5 # minutes

              pilot = pm.submit_pilots(pd)
    '''

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
    def __init__(self, pmgr, descr):

        # 'static' members
        self._descr = descr.as_dict()

        # sanity checks on description
        for check in ['resource', 'cores', 'runtime']:
            if not self._descr.get(check):
                raise ValueError("ComputePilotDescription needs '%s'" % check)

        # initialize state
        self._pmgr       = pmgr
        self._session    = self._pmgr.session
        self._prof       = self._session._prof
        self._uid        = ru.generate_id('pilot.%(item_counter)04d',
                                           ru.ID_CUSTOM,
                                           ns=self._session.uid)
        self._state      = rps.NEW
        self._log        = pmgr._log
        self._pilot_dict = dict()
        self._callbacks  = dict()
        self._cache      = dict()    # cache of SAGA dir handles
        self._cb_lock    = ru.RLock()

        # pilot failures can trigger app termination
        self._exit_on_error = self._descr.get('exit_on_error')

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
        self._resource_sandbox = ru.Url()
        self._session_sandbox  = ru.Url()
        self._pilot_sandbox    = ru.Url()
        self._client_sandbox   = ru.Url()

        pilot = self.as_dict()

        self._pilot_jsurl, self._pilot_jshop \
                               = self._session._get_jsurl           (pilot)
        self._resource_sandbox = self._session._get_resource_sandbox(pilot)
        self._session_sandbox  = self._session._get_session_sandbox (pilot)
        self._pilot_sandbox    = self._session._get_pilot_sandbox   (pilot)
        self._client_sandbox   = self._session._get_client_sandbox()

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

        self._resource_sandbox.path  = self._resource_sandbox.path % expand
        self._session_sandbox .path  = self._session_sandbox .path % expand
        self._pilot_sandbox   .path  = self._pilot_sandbox   .path % expand


    # --------------------------------------------------------------------------
    #
    def __repr__(self):

        return str(self)


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str([self.uid, self.resource, self.state])


    # --------------------------------------------------------------------------
    #
    def _default_state_cb(self, pilot, state):

        self._log.info("[Callback]: pilot %s state: %s.", self.uid, self.state)

        if self.state == rps.FAILED and self._exit_on_error:
            self._log.error("[Callback]: pilot '%s' failed - exit", self.uid)

            # There are different ways to tell main...
            ru.cancel_main_thread('int')
          # raise RuntimeError('pilot %s failed - fatal!' % self.uid)
          # os.kill(os.getpid())
          # sys.exit()


    # --------------------------------------------------------------------------
    #
    def _update(self, pilot_dict):
        '''
        This will update the facade object after state changes etc, and is
        invoked by whatever component receiving that updated information.

        Return True if state changed, False otherwise
        '''

        self._log.debug('update %s', pilot_dict['uid'])

        if pilot_dict['uid'] != self.uid:
            self._log.error('invalid uid: %s / %s', pilot_dict['uid'], self.uid)

        assert(pilot_dict['uid'] == self.uid), 'update called on wrong instance'

        # NOTE: this method relies on state updates to arrive in order and
        #       without gaps.
        current = self.state
        target  = pilot_dict['state']

        if target not in [rps.FAILED, rps.CANCELED]:

            try:
                cur_state_val = rps._pilot_state_value(current)
                tgt_state_val = rps._pilot_state_value(target)
                assert(tgt_state_val - cur_state_val), 'invalid state transition'

            except:
                self._log.error('%s: invalid state transition %s -> %s',
                        self.uid, current, target)
                raise

        self._state = target

        # keep all information around
        self._pilot_dict = copy.deepcopy(pilot_dict)

        # invoke pilot specific callbacks
        # FIXME: this iteration needs to be thread-locked!
        for _,cb_val in self._callbacks[rpc.PILOT_STATE].items():

            cb      = cb_val['cb']
            cb_data = cb_val['cb_data']

            self._log.debug('call %s', cb)

            self._log.debug('%s calls cb %s', self.uid, cb)

            if cb_data: cb(self, self.state, cb_data)
            else      : cb(self, self.state)

        # ask pmgr to invoke any global callbacks
        self._pmgr._call_pilot_callbacks(self, self.state)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        '''
        Returns a Python dictionary representation of the object.
        '''

        ret = {'session':          self.session.uid,
               'pmgr':             self.pmgr.uid,
               'uid':              self.uid,
               'type':             'pilot',
               'state':            self.state,
               'log':              self.log,
               'stdout':           self.stdout,
               'stderr':           self.stderr,
               'resource':         self.resource,
               'resource_sandbox': str(self._resource_sandbox),
               'session_sandbox':  str(self._session_sandbox),
               'pilot_sandbox':    str(self._pilot_sandbox),
               'client_sandbox':   str(self._client_sandbox),
               'js_url':           str(self._pilot_jsurl),
               'js_hop':           str(self._pilot_jshop),
               'description':      self.description,  # this is a deep copy
               'resource_details': self.resource_details
              }

        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        '''
        Returns the pilot's session.

        **Returns:**
            * A :class:`Session`.
        '''

        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def pmgr(self):
        '''
        Returns the pilot's manager.

        **Returns:**
            * A :class:`PilotManager`.
        '''

        return self._pmgr


    # -------------------------------------------------------------------------
    #
    @property
    def resource_details(self):
        '''
        Returns agent level resource information
        '''

        return self._pilot_dict.get('resource_details')


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        '''
        Returns the pilot's unique identifier.

        The uid identifies the pilot within a :class:`PilotManager`.

        **Returns:**
            * A unique identifier (string).
        '''

        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        '''
        Returns the current state of the pilot.

        **Returns:**
            * state (string enum)
        '''

        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def log(self):
        '''
        Returns a list of human readable [timestamp, string] tuples describing
        various events during the pilot's lifetime.  Those strings are not
        normative, only informative!

        **Returns:**
            * log (list of [timestamp, string] tuples)
        '''

        return self._pilot_dict.get('log')


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        '''
        Returns a snapshot of the pilot's STDOUT stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stdout (string)
        '''

        return self._pilot_dict.get('stdout')


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        '''
        Returns a snapshot of the pilot's STDERR stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stderr (string)
        '''

        return self._pilot_dict.get('stderr')


    # --------------------------------------------------------------------------
    #
    @property
    def resource(self):
        '''
        Returns the resource tag of this pilot.

        **Returns:**
            * A resource tag (string)
        '''

        return self._descr.get('resource')


    # --------------------------------------------------------------------------
    #
    @property
    def pilot_sandbox(self):
        '''
        Returns the full sandbox URL of this pilot, if that is already
        known, or 'None' otherwise.

        **Returns:**
            * A string
        '''

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
    def resource_sandbox(self):
        return self._resource_sandbox


    @property
    def session_sandbox(self):
        return self._session_sandbox

    @property
    def client_sandbox(self):
        return self._client_sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        '''
        Returns the description the pilot was started with, as a dictionary.

        **Returns:**
            * description (dict)
        '''

        return copy.deepcopy(self._descr)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, metric=rpc.PILOT_STATE, cb_data=None):
        '''
        Registers a callback function that is triggered every time the
        pilot's state changes.

        All callback functions need to have the same signature::

            def cb(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If 'cb_data' is given,
        then the 'cb' signature changes to

            def cb(obj, state, cb_data)

        and 'cb_data' are passed along.

        '''

        if metric not in rpc.PMGR_METRICS :
            raise ValueError ("Metric '%s' not available on pmgr" % metric)

        with self._cb_lock:
            cb_name = cb.__name__
            self._callbacks[metric][cb_name] = {'cb'      : cb,
                                                'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb, metric=rpc.PILOT_STATE):

        if metric and metric not in rpc.UMGR_METRICS :
            raise ValueError ("Metric '%s' not available on pmgr" % metric)

        if   not metric                  : metrics = rpc.PMGR_METRICS
        elif not isinstance(metric, list): metrics = [metric]
        else                             : metrics = metric

        with self._cb_lock:

            for metric in metrics:

                if cb: to_delete = [cb.__name__]
                else : to_delete = list(self._callbacks[metric].keys())

                for cb_name in to_delete:

                    if cb_name not in self._callbacks[metric]:
                        raise ValueError("Callback '%s' is not registered" % cb_name)

                    del(self._callbacks[metric][cb_name])


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        '''
        Returns when the pilot reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that pilot has to reach in order for the
              call to return.

              By default `wait` waits for the pilot to reach a **final**
              state, which can be one of the following:

              * :data:`radical.pilot.states.DONE`
              * :data:`radical.pilot.states.FAILED`
              * :data:`radical.pilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the pilot has reached the desired state or not.  The
              default value **None** never times out.  '''

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
        '''
        Cancel the pilot.
        '''

        # clean connection cache
        try:
            for key in self._cache:
                self._cache[key].close()
            self._cache = dict()

        except:
            pass

        self._pmgr.cancel_pilots(self.uid)


    # --------------------------------------------------------------------------
    #
    def stage_in(self, directives):
        '''
        Stages the content of the staging directive into the pilot's
        staging area
        '''

        # This staging request is actually served by the pmgr *launching*
        # component, because that already has a channel open to the target
        # resource which we can reuse.  We might eventually implement or
        # interface to a dedicated data movement service though.

        # send the staging request to the pmg launcher
        self._pmgr._pilot_staging_input(self.as_dict(), directives)


    # --------------------------------------------------------------------------
    #
    def stage_out(self):
        '''
        fetch `staging_output.tgz` from the pilot sandbox, and store in $PWD
        '''

        try:
            psbox = self._session.get_fs_dir(self._pilot_sandbox)
            psbox.copy('staging_output.tgz', self._client_sandbox)

        except Exception:
            self._log.exception('output staging failed')
            raise


# ------------------------------------------------------------------------------

