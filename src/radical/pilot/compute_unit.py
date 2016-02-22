
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time

import radical.utils as ru

from . import utils     as rpu
from . import states    as rps
from . import constants as rpc
from . import types     as rpt

from .staging_directives import expand_description


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
                      ud.cores      = 1

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

        # 'static' members
        self._descr = descr.as_dict()
        self._umgr  = umgr

        # initialize state
        self._session    = self._umgr.session
        self._uid        = ru.generate_id('unit.%(counter)06d', ru.ID_CUSTOM)
        self._state      = rps.NEW
        self._state_hist = [[rps.NEW, rpu.timestamp()]]
        self._log        = umgr._log
        self._log_msgs   = []
        self._exit_code  = None
        self._stdout     = None
        self._stderr     = None
        self._pilot      = None
        self._sandbox    = None

        # sanity checks on description
        for check in ['cores']:
            if not self._descr.get(check):
                raise ValueError("ComputeUnitDescription needs '%s'" % check)

        if  not self._descr.get('executable') and \
            not self._descr.get('kernel')     :
            raise ValueError("ComputeUnitDescription needs 'executable' or 'kernel'")

        # If staging directives exist, expand them
        expand_description(self._descr)


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

        return self.as_dict()


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """
        Returns a Python dictionary representation of the object.
        """
        
        ret = {
            'session':         self.session.uid,
            'umgr':            self.umgr.uid,
            'uid':             self.uid,
            'type':            'unit',
            'name':            self.name,
            'state':           self.state,
            'state_history':   self.state_history,
            'log':             self.log,
            'exit_code':       self.exit_code,
            'stdout':          self.stdout,
            'stderr':          self.stderr,
            'pilot':           self.pilot,
            'sandbox':         self.sandbox,
            'description':     copy.deepcopy(self.description.as_dict())
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
    def state_history(self):
        """
        Returns the complete state history of the unit.

        **Returns:**
            * list of tuples [[state, timestamp]]
        """

        return copy.deepcopy(self._state_hist)


    # --------------------------------------------------------------------------
    #
    @property
    def log(self):
        """
        Returns a list of human readable [timestamp, string] tuples describing
        various events during the unit's lifetime.  Those strings are not
        normative, only informative!

        **Returns:**
            * log (list of [timestamp, string] tuples)
        """

        return copy.deepcopy(self._log_msgs)


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
    def working_directory(self):
        """
        Returns the full working directory URL of this unit, if that is
        already known, or 'None' otherwise

        **Returns:**
            * A URL (radical.utils.Url).

        **NOTE:** deprecated, use *`sandbox`*
        """

        return self.sandbox


    # --------------------------------------------------------------------------
    #
    @property
    def sandbox(self):
        """
        Returns the full sandbox URL of this unit, if that is already
        known, or 'None' otherwise.

        **Returns:**
            * A URL (radical.utils.Url).
        """

        return self._sandbox


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
    def register_callback(self, cb_func, cb_data=None):
        """
        Registers a callback function that is triggered every time the
        unit's state changes.

        All callback functions need to have the same signature::

            def cb_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.  If 'cb_data' is given,
        then the 'cb_func' signature changes to 

            def cb_func(obj, state, cb_data)

        and 'cb_data' are passed along.

        """
        self._umgr.register_callback(self.uid, rpt.UNIT_STATE, cb_func, cb_data)


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

        return self.state


    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """
        Cancel the unit.
        """
        
        self._umgr._cancel_unit(self.uid)

# ------------------------------------------------------------------------------

