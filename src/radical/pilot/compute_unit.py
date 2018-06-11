
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time
import threading

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc
from . import types     as rpt

from . import compute_unit_description as cud

from .staging_directives import expand_description
from .staging_directives import TRANSFER, COPY, LINK, MOVE, STAGING_AREA


# ------------------------------------------------------------------------------
#
class ComputeUnit(object):
    """
    A ComputeUnit represent a 'task' that is executed on a ComputePilot.
    ComputeUnits allow to control and query the state of this task.

    .. note:: A unit cannot be created directly. The factory method
              :meth:`radical.pilot.UnitManager.submit_units` has to be used instead.

                **Example**::

                      umgr = radical.pilot.UnitManager(session=s)

                      ud = radical.pilot.ComputeUnitDescription()
                      ud.executable = "/bin/date"

                      unit = umgr.submit_units(ud)
    """

    # --------------------------------------------------------------------------
    # In terms of implementation, a CU is not much more than a dict whose
    # content are dynamically updated to reflect the state progression through
    # the UMGR components.  As a CU is always created via a UMGR, it is
    # considered to *belong* to that UMGR, and all activities are actually
    # implemented by that UMGR.
    #
    # Note that this implies that we could create CUs before submitting them
    # to a UMGR, w/o any problems. (FIXME?)
    # --------------------------------------------------------------------------


    # --------------------------------------------------------------------------
    #
    def __init__(self, umgr, descr):

        # FIXME GPU: we allow `mpi` for backward compatibility - but need to
        #       convert the bool into a decent value for `cpu_process_type`
        if  descr[cud.CPU_PROCESS_TYPE] in [True, 'True']:
            descr[cud.CPU_PROCESS_TYPE] = cud.MPI

        # 'static' members
        self._descr = descr.as_dict()
        self._umgr  = umgr

        # initialize state
        self._session          = self._umgr.session
        self._uid              = ru.generate_id('unit.%(counter)06d', ru.ID_CUSTOM)
        self._state            = rps.NEW
        self._log              = umgr._log
        self._exit_code        = None
        self._stdout           = None
        self._stderr           = None
        self._pilot            = descr.get('pilot')
        self._resource_sandbox = None
        self._pilot_sandbox    = None
        self._unit_sandbox     = None
        self._client_sandbox   = None
        self._callbacks        = dict()
        self._cb_lock          = threading.RLock()

        for m in rpt.UMGR_METRICS:
            self._callbacks[m] = dict()

        # we always invke the default state cb
        self._callbacks[rpt.UNIT_STATE][self._default_state_cb.__name__] = {
                'cb'      : self._default_state_cb, 
                'cb_data' : None}

        if  not self._descr.get('executable') and \
            not self._descr.get('kernel')     :
            raise ValueError("CU description needs 'executable' or 'kernel'")

        # If staging directives exist, expand them to the full dict version.  Do
        # not, however, expand any URLs as of yet, as we likely don't have
        # sufficient information about pilot sandboxes etc.
        expand_description(self._descr)

        self._umgr.advance(self.as_dict(), rps.NEW, publish=False, push=False)


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def create(umgr, descr):
        """ 
        PRIVATE: Create a new compute unit (in NEW state)
        """

        return ComputeUnit(umgr=umgr, descr=descr)


    # --------------------------------------------------------------------------
    #
    def __repr__(self):

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return [self.uid, self.pilot, self.state]


    # --------------------------------------------------------------------------
    #
    def _default_state_cb(self, unit, state):

        self._log.info("[Callback]: unit %s state: %s.", self.uid, self.state)


    # --------------------------------------------------------------------------
    #
    def _update(self, unit_dict):
        """
        This will update the facade object after state changes etc, and is
        invoked by whatever component receiving that updated information.

        Return True if state changed, False otherwise
        """

        assert(unit_dict['uid'] == self.uid), 'update called on wrong instance'

        # NOTE: this method relies on state updates to arrive in order, and
        #       without gaps.
        current = self.state
        target  = unit_dict['state']

        if target not in [rps.FAILED, rps.CANCELED]:
            try:
                assert(rps._unit_state_value(target) - rps._unit_state_value(current) == 1), \
                            'invalid state transition'
            except:
                self._log.error('%s: invalid state transition %s -> %s',
                                self.uid, current, target)
                raise

        self._state = target

        # we update all fields
        # FIXME: well, not all really :/
        # FIXME: setattr is ugly...  we should maintain all state in a dict.
        for key in ['state', 'stdout', 'stderr', 'exit_code', 'pilot', 
                    'resource_sandbox', 'pilot_sandbox', 'unit_sandbox', 
                    'client_sandbox']:

            val = unit_dict.get(key, None)
            if val != None:
                setattr(self, "_%s" % key, val)

        # invoke unit specific callbacks
        for cb_name, cb_val in self._callbacks[rpt.UNIT_STATE].iteritems():

            cb      = cb_val['cb']
            cb_data = cb_val['cb_data']

            self._log.debug('%s calls state cb %s', self.uid, cb)

            if cb_data: cb(self, self.state, cb_data)
            else      : cb(self, self.state)

        # ask umgr to invoke any global callbacks
        self._umgr._call_unit_callbacks(self, self.state)


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a Python dictionary representation of the object.
        """

        ret = {
            'type':             'unit',
            'umgr':             self.umgr.uid,
            'uid':              self.uid,
            'name':             self.name,
            'state':            self.state,
            'exit_code':        self.exit_code,
            'stdout':           self.stdout,
            'stderr':           self.stderr,
            'pilot':            self.pilot,
            'resource_sandbox': self.resource_sandbox,
            'pilot_sandbox':    self.pilot_sandbox,
            'unit_sandbox':     self.unit_sandbox,
            'client_sandbox':   self.client_sandbox,
            'description':      self.description   # this is a deep copy
        }

        return ret


    # --------------------------------------------------------------------------
    #
    @property
    def session(self):
        """
        Returns the unit's session.

        **Returns:**
            * A :class:`Session`.
        """

        return self._session


    # --------------------------------------------------------------------------
    #
    @property
    def umgr(self):
        """
        Returns the unit's manager.

        **Returns:**
            * A :class:`UnitManager`.
        """

        return self._umgr


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """
        Returns the unit's unique identifier.

        The uid identifies the unit within a :class:`UnitManager`.

        **Returns:**
            * A unique identifier (string).
        """
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        """
        Returns the unit's application specified name.

        **Returns:**
            * A name (string).
        """
        return self._descr.get('name')


    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """
        Returns the current state of the unit.

        **Returns:**
            * state (string enum)
        """

        return self._state


    # --------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        """
        Returns the exit code of the unit, if that is already known, or
        'None' otherwise.

        **Returns:**
            * exit code (int)
        """

        return self._exit_code


    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """
        Returns a snapshot of the executable's STDOUT stream.

        If this property is queried before the unit has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stdout (string)
        """

        return self._stdout


    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """
        Returns a snapshot of the executable's STDERR stream.

        If this property is queried before the unit has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can be inefficient.  Output may be incomplete and/or
           filtered.

        **Returns:**
            * stderr (string)
        """

        return self._stderr


    # --------------------------------------------------------------------------
    #
    @property
    def pilot(self):
        """
        Returns the pilot ID of this unit, if that is already known, or
        'None' otherwise.

        **Returns:**
            * A pilot ID (string)
        """

        return self._pilot


    # --------------------------------------------------------------------------
    #
    @property
    def working_directory(self): # **NOTE:** deprecated, use *`sandbox`*
        return self.sandbox

    @property
    def sandbox(self):
        return self.unit_sandbox

    @property
    def unit_sandbox(self):
        """
        Returns the full sandbox URL of this unit, if that is already
        known, or 'None' otherwise.

        **Returns:**
            * A URL (radical.utils.Url).
        """

        # NOTE: The unit has a sandbox property, containing the full sandbox
        #       path, which is used by the umgr to stage data back and forth.
        #       However, the full path as visible from the umgr side might not
        #       be what the agent is seeing, specifically in the case of
        #       non-shared filesystems (OSG).  The agent thus uses
        #       `$PWD/cu['uid']` as sandbox, with the assumption that this will
        #       get mapped to whatever is here returned as sandbox URL.  
        #
        #       There is thus implicit knowledge shared between the RP client
        #       and the RP agent on how the sandbox path is formed!

        return self._unit_sandbox


    @property
    def resource_sandbox(self):
        return self._resource_sandbox

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
        """
        Returns the description the unit was started with, as a dictionary.

        **Returns:**
            * description (dict)
        """

        return copy.deepcopy(self._descr)


    # --------------------------------------------------------------------------
    #
    def register_callback(self, cb, cb_data=None):
        """
        Registers a callback function that is triggered every time the
        unit's state changes.

        All callback functions need to have the same signature::

            def cb(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If 'cb_data' is given,
        then the 'cb' signature changes to 

            def cb(obj, state, cb_data)

        and 'cb_data' are passed along.

        """
        self._umgr.register_callback(self.uid, rpt.UNIT_STATE, cb, cb_data)


    # --------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        """
        Returns when the unit reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that unit has to reach in order for the
              call to return.

              By default `wait` waits for the unit to reach a **final**
              state, which can be one of the following:

              * :data:`radical.pilot.states.DONE`
              * :data:`radical.pilot.states.FAILED`
              * :data:`radical.pilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the unit has reached the desired state or not.  The
              default value **None** never times out.  """

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
            # raise RuntimeError("can't wait on a unit in final state")
            return self.state

        start_wait = time.time()
        while self.state not in states:

            time.sleep(0.1)

            if timeout and (timeout <= (time.time() - start_wait)):
                break

          # if self._umgr._terminate.is_set():
          #     break

        return self.state


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """
        Cancel the unit.
        """
        
        self._umgr.cancel_units(self.uid)


# ------------------------------------------------------------------------------

