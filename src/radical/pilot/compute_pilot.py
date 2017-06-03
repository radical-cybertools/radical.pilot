
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import copy
import time
import saga
import threading

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc
from . import types     as rpt

from .staging_directives import expand_staging_directives
from .staging_directives import TRANSFER, COPY, LINK, MOVE, STAGING_AREA


# ------------------------------------------------------------------------------
#
class ComputePilot(object):
    """
    A ComputePilot represent a resource overlay on a local or remote resource.

    .. note:: A ComputePilot cannot be created directly. The factory method
              :meth:`radical.pilot.PilotManager.submit_pilots` has to be used instead.

                **Example**::

                      pm = radical.pilot.PilotManager(session=s)

                      pd = radical.pilot.ComputePilotDescription()
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
    def __init__(self, pmgr, descr):

        # 'static' members
        self._descr = descr.as_dict()

        # sanity checks on description
        for check in ['resource', 'cores', 'runtime']:
            if not self._descr.get(check):
                raise ValueError("ComputePilotDescription needs '%s'" % check)

        # initialize state
        self._pmgr          = pmgr
        self._session       = self._pmgr.session
        self._uid           = ru.generate_id('pilot.%(counter)04d', ru.ID_CUSTOM)
        self._state         = rps.NEW
        self._log           = pmgr._log
        self._pilot_dict    = dict()
        self._callbacks     = dict()
        self._cb_lock       = threading.RLock()
        self._exit_on_error = self._descr.get('exit_on_error')

        for m in rpt.PMGR_METRICS:
            self._callbacks[m] = dict()

        # we always invoke the default state cb
        self._callbacks[rpt.PILOT_STATE][self._default_state_cb.__name__] = {
                'cb'      : self._default_state_cb, 
                'cb_data' : None}

        # `as_dict()` needs `pilot_dict` and other attributes.  Those should all
        # be available at this point (apart from the sandbox itself), so we now
        # query for that sandbox.
        self._sandbox       = None
        self._sandbox       = self._session._get_pilot_sandbox(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def __repr__(self):

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return [self.uid, self.resource, self.state]


    # --------------------------------------------------------------------------
    #
    def _default_state_cb(self, pilot, state):

        self._log.info("[Callback]: pilot %s state: %s.", self.uid, self.state)

        if self.state == rps.FAILED and self._exit_on_error:
            self._log.error("[Callback]: pilot '%s' failed", self.uid)
            # FIXME: how to tell main?  Where are we in the first place?
          # ru.cancel_main_thread('int')
            raise RuntimeError('pilot %s failed - fatal!' % self.uid)
          # sys.exit()


    # --------------------------------------------------------------------------
    #
    def _update(self, pilot_dict):
        """
        This will update the facade object after state changes etc, and is
        invoked by whatever component receiving that updated information.

        Return True if state changed, False otherwise
        """

        assert(pilot_dict['uid'] == self.uid), 'update called on wrong instance'

        # NOTE: this method relies on state updates to arrive in order, and
        #       without gaps.
        current = self.state
        target  = pilot_dict['state']

        if target not in [rps.FAILED, rps.CANCELED]:
            assert(rps._pilot_state_value(target) - rps._pilot_state_value(current)), \
                            'invalid state transition'
            # FIXME

        self._state = target

        # keep all information around
        self._pilot_dict = copy.deepcopy(pilot_dict)

        # invoke pilot specific callbacks
        for cb_name, cb_val in self._callbacks[rpt.PILOT_STATE].iteritems():

            cb      = cb_val['cb']
            cb_data = cb_val['cb_data']

          # print ' ~~~ call pcbs: %s -> %s : %s' % (self.uid, self.state, cb_name)
            
            if cb_data: cb(self, self.state, cb_data)
            else      : cb(self, self.state)

        # ask pmgr to invoke any global callbacks
        self._pmgr._call_pilot_callbacks(self, self.state)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a Python dictionary representation of the object.
        """
        ret = {
            'session':          self.session.uid,
            'pmgr':             self.pmgr.uid,
            'uid':              self.uid,
            'type':             'pilot',
            'state':            self.state,
            'log':              self.log,
            'stdout':           self.stdout,
            'stderr':           self.stderr,
            'resource':         self.resource,
            'sandbox':          str(self.sandbox), # for mongodb insertion
            'description':      self.description,  # this is a deep copy
            'resource_details': self.resource_details
        }
        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        """
        Returns the pilot's session.

        **Returns:**
            * A :class:`Session`.
        """

        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def pmgr(self):
        """
        Returns the pilot's manager.

        **Returns:**
            * A :class:`PilotManager`.
        """

        return self._pmgr


    # -------------------------------------------------------------------------
    #
    @property
    def resource_details(self):
        """
        Returns agent level resource information
        """
        return self._pilot_dict.get('resource_details')


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """
        Returns the pilot's unique identifier.

        The uid identifies the pilot within a :class:`PilotManager`.

        **Returns:**
            * A unique identifier (string).
        """
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """
        Returns the current state of the pilot.

        **Returns:**
            * state (string enum)
        """

        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def log(self):
        """
        Returns a list of human readable [timestamp, string] tuples describing
        various events during the pilot's lifetime.  Those strings are not
        normative, only informative!

        **Returns:**
            * log (list of [timestamp, string] tuples)
        """

        return self._pilot_dict.get('log')


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """
        Returns a snapshot of the pilot's STDOUT stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stdout (string)
        """

        return self._pilot_dict.get('stdout')


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """
        Returns a snapshot of the pilot's STDERR stream.

        If this property is queried before the pilot has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stderr (string)
        """

        return self._pilot_dict.get('stderr')


    # --------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """
        Returns the resource tag of this pilot.

        **Returns:**
            * A resource tag (string)
        """

        return self._descr.get('resource')


    # --------------------------------------------------------------------------
    #
    @property
    def sandbox(self):
        """
        Returns the full sandbox URL of this pilot, if that is already
        known, or 'None' otherwise.

        **Returns:**
            * A URL (radical.utils.Url).
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

        return self._sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        """
        Returns the description the pilot was started with, as a dictionary.

        **Returns:**
            * description (dict)
        """

        return copy.deepcopy(self._descr)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, metric=rpt.PILOT_STATE, cb_data=None):
        """
        Registers a callback function that is triggered every time the
        pilot's state changes.

        All callback functions need to have the same signature::

            def cb(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If 'cb_data' is given,
        then the 'cb' signature changes to 

            def cb(obj, state, cb_data)

        and 'cb_data' are passed along.

        """
        if metric not in rpt.PMGR_METRICS :
            raise ValueError ("Metric '%s' is not available on the pilot manager" % metric)

        with self._cb_lock:
            cb_name = cb.__name__
            self._callbacks[metric][cb_name] = {'cb'      : cb, 
                                                'cb_data' : cb_data}


    # --------------------------------------------------------------------------
    #
    def unregister_callback(self, cb, metric=rpt.PILOT_STATE):

        if metric and metric not in rpt.UMGR_METRICS :
            raise ValueError ("Metric '%s' is not available on the pilot manager" % metric)

        if not metric:
            metrics = rpt.PMGR_METRICS
        elif isinstance(metric, list):
            metrics =  metric
        else:
            metrics = [metric]

        with self._cb_lock:

            for metric in metrics:

                if cb:
                    to_delete = [cb.__name__]
                else:
                    to_delete = self._callbacks[metric].keys()

                for cb_name in to_delete:

                    if cb_name not in self._callbacks[metric]:
                        raise ValueError("Callback '%s' is not registered" % cb_name)

                    del(self._callbacks[metric][cb_name])


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        """
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
              default value **None** never times out.  """

        if not state:
            states = rps.FINAL
        elif not isinstance(state, list):
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
            # raise RuntimeError("can't wait on a pilot in final state")
            return self.state

        start_wait = time.time()
        while self.state not in states:

            time.sleep(0.1)
            if timeout and (timeout <= (time.time() - start_wait)):
                break

          # if self._pmgr._terminate.is_set():
          #     break

        return self.state


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """
        Cancel the pilot.
        """
        
      # print 'pilot: cancel'
        self._pmgr.cancel_pilots(self.uid)


    # --------------------------------------------------------------------------
    #
    def stage_in(self, directives):
        """Stages the content of the staging directive into the pilot's
        staging area"""

        # Wait until we can assume the pilot directory to be created
        # # FIXME: can't we create it ourself?
        if self.state == rps.NEW:
            self.wait(state=[rps.PMGR_LAUNCHING_PENDING, rps.PMGR_LAUNCHING,
                             rps.PMGR_ACTIVE_PENDING,    rps.PMGR_ACTIVE])
        elif self.state in rps.FINAL:
            raise Exception("Pilot already finished, no need to stage anymore!")

        # Iterate over all directives
        for directive in expand_staging_directives(directives):

            # TODO: respect flags in directive

            src_url = saga.Url(directive['source'])
            action = directive['action']

            # Convert the target url into a SAGA Url object
            tgt_url = saga.Url(directive['target'])
            # Create a pointer to the directory object that we will use
            tgt_dir_url = tgt_url

            if tgt_url.path.endswith('/'):
                # If the original target was a directory (ends with /),
                # we assume that the user wants the same filename as the source.
                tgt_filename = os.path.basename(src_url.path)
            else:
                # Otherwise, extract the filename and update the directory
                tgt_filename = os.path.basename(tgt_dir_url.path)
                tgt_dir_url.path = os.path.dirname(tgt_dir_url.path)

            # Handle special 'staging' schema
            if tgt_dir_url.schema == 'staging':

                # We expect a staging:///relative/path/file.txt URI,
                # as hostname would have unclear semantics currently.
                if tgt_dir_url.host:
                    raise Exception("hostname not supported with staging:// schema")

                # Remove the leading slash to get a relative path from the staging area
                rel_path = os.path.relpath(tgt_dir_url.path, '/')

                # Now base the target directory relative of the sandbox and staging prefix
                tgt_dir_url      = saga.Url(self.sandbox)
                tgt_dir_url.path = os.path.join(self.sandbox.path, 
                                                STAGING_AREA, rel_path)

            # Define and open the staging directory for the pilot
            # We use the target dir construct here, so that we can create
            # the directory if it does not yet exist.
            target_dir = saga.filesystem.Directory(tgt_dir_url, flags=saga.filesystem.CREATE_PARENTS)

            if action == LINK:	
                # TODO: Does this make sense?
                #log_message = 'Linking %s to %s' % (source, abs_target)
                #os.symlink(source, abs_target)
                self._log.error("action 'LINK' not supported on pilot level staging")
                raise ValueError("action 'LINK' not supported on pilot level staging")

            elif action == COPY:
                # TODO: Does this make sense?
                #log_message = 'Copying %s to %s' % (source, abs_target)
                #shutil.copyfile(source, abs_target)
                self._log.error("action 'COPY' not supported on pilot level staging")
                raise ValueError("action 'COPY' not supported on pilot level staging")

            elif action == MOVE:
                # TODO: Does this make sense?
                #log_message = 'Moving %s to %s' % (source, abs_target)
                #shutil.move(source, abs_target)
                self._log.error("action 'MOVE' not supported on pilot level staging")
                raise ValueError("action 'MOVE' not supported on pilot level staging")

            elif action == TRANSFER:
                self._log.info('Transferring %s to %s/%s', 
                               src_url, tgt_dir_url, tgt_filename)
                # Transfer the source file to the target staging area
                target_dir.copy(src_url, tgt_filename)
            else:
                raise Exception('Action %s not supported' % action)

# ------------------------------------------------------------------------------

