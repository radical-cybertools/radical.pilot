#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.compute_unit
   :platform: Unix
   :synopsis: Implementation of the ComputeUnit class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time

from radical.pilot.utils.logger import logger

from radical.pilot.states import *
from radical.pilot.exceptions import *

# -----------------------------------------------------------------------------
#
class ComputeUnit(object):
    """A ComputeUnit represent a 'task' that is executed on a ComputePilot.
    ComputeUnits allow to control and query the state of this task.

    .. note:: A ComputeUnit cannot be created directly. The factory method
              :meth:`radical.pilot.UnitManager.submit_units` has to be used instead.

                **Example**::

                      umgr = radical.pilot.UnitManager(session=s)

                      ud = radical.pilot.ComputeUnitDescription()
                      ud.executable = "/bin/date"
                      ud.cores      = 1

                      unit = umgr.submit_units(ud)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self):
        """ Le constructeur. Not meant to be called directly.
        """
        # 'static' members
        self._uid = None
        self._description = None
        self._manager = None

        # handle to the manager's worker
        self._worker = None

        if os.getenv("RADICALPILOT_GCDEBUG", None) is not None:
            logger.debug("GCDEBUG __init__(): ComputeUnit [object id: %s]." % id(self))

    #--------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        if os.getenv("RADICALPILOT_GCDEBUG", None) is not None:
            logger.debug("GCDEBUG __del__(): ComputeUnit [object id: %s]." % id(self))

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def _create(unit_manager_obj, unit_uid, unit_description):
        """ PRIVATE: Create a new compute unit.
        """
        # create and return pilot object
        computeunit = ComputeUnit()
        computeunit._uid = unit_uid

        computeunit._description = unit_description
        computeunit._manager = unit_manager_obj

        computeunit._worker = unit_manager_obj._worker
        return computeunit

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def _get(unit_manager_obj, unit_ids):
        """ PRIVATE: Get one or more pilot via their UIDs.
        """
        units_json = unit_manager_obj._session._dbs.get_compute_units(
            unit_manager_id=unit_manager_obj.uid,
            unit_ids=unit_ids
        )
        # create and return pilot objects
        computeunits = []

        for u in units_json:
            computeunit = ComputeUnit()
            computeunit._uid = str(u['_id'])
            computeunit._description = u['description']
            computeunit._manager = unit_manager_obj
            computeunit._worker = unit_manager_obj._worker

            computeunits.append(computeunit)

        return computeunits

    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        obj_dict = {
            'uid':               self.uid,
            'state':             self.state,
            'exit_code':         self.exit_code,
            'log':               self.log,
            'execution_details': self.execution_details,
            'submission_time':   self.submission_time,
            'working_directory': self.working_directory,
            'start_time':        self.start_time,
            'stop_time':         self.stop_time
        }
        return obj_dict

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        return str(self.as_dict())

    # -------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the Pilot's unique identifier.

        The uid identifies the ComputePilot within a :class:`PilotManager` and
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        # uid is static and doesn't change over the lifetime
        # of a pilot, hence it can be stored in a member var.
        return self._uid

    # -------------------------------------------------------------------------
    #
    @property
    def working_directory(self):
        """Returns the full working directory URL of this ComputeUnit.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['sandbox']

    # -------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """Returns a snapshot of the executable's STDOUT stream.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can become very inefficient for lareg data volumes.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        return self._worker.get_compute_unit_stdout(self.uid)

    # -------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """Returns a snapshot of the executable's STDERR stream.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.

        .. warning: This can become very inefficient for large data volumes.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        return self._worker.get_compute_unit_stderr(self.uid)

    # -------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the pilot description the ComputeUnit was started with.
        """
        # description is static and doesn't change over the lifetime
        # of a pilot, hence it is stored as a member var.
        return self._description

    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the ComputeUnit.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['state']

    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """Returns the complete state history of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        states = []

        cu_json = self._worker.get_compute_unit_data(self.uid)
        for state in cu_json['statehistory']:
            states.append(State(state=state["state"], timestamp=state["timestamp"]))

        return states

    # -------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        """Returns the exit code of the ComputeUnit.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['exit_code']

    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the logs of the ComputeUnit.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['log']

    # -------------------------------------------------------------------------
    #
    @property
    def execution_details(self):
        """Returns the exeuction location(s) of the ComputeUnit.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['exec_locs']

    # -------------------------------------------------------------------------
    #
    @property
    def execution_locations(self):
        """Returns the exeuction location(s) of the ComputeUnit.
           This is just an alias for execution_details.
        """
        return self.execution_details

    # -------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the ComputeUnit was submitted.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['submitted']

    # -------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the ComputeUnit was started on the backend.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['started']

    # -------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the ComputeUnit was stopped.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['finished']

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func):
        """Registers a callback function that is triggered every time the
        ComputeUnit's state changes.

        All callback functions need to have the same signature::

            def callback_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """
        self._worker.register_unit_callback(self, callback_func)

    # -------------------------------------------------------------------------
    #
    def wait(self, state=[DONE, FAILED, CANCELED],
             timeout=None):
        """Returns when the ComputeUnit reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that compute unit has to reach in order for the
              call to return.

              By default `wait` waits for the compute unit to reach
              a **terminal** state, which can be one of the following:

              * :data:`radical.pilot.states.DONE`
              * :data:`radical.pilot.states.FAILED`
              * :data:`radical.pilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the compute unit has reached the desired state or not.
              The default value **None** never times out.

        **Raises:**
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        if not isinstance(state, list):
            state = [state]

        start_wait = time.time()
        # the self.state property pulls the state from the back end.
        new_state = self.state
        while new_state not in state:
            time.sleep(1)

            new_state = self.state
            logger.debug(
                "Compute unit %s in state %s" % (self._uid, new_state))

            if(None != timeout) and (timeout <= (time.time() - start_wait)):
                break

        # done waiting
        return

    # -------------------------------------------------------------------------
    #
    def cancel(self):
        """Terminates the ComputeUnit.

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.radical.pilotException(
                "Invalid Compute Unit instance.")

        if self.state in [DONE, FAILED, CANCELED]:
            # nothing to do
            return

        if self.state in [UNKNOWN]:
            raise exceptions.radical.pilotException(
                "Compute Unit state is UNKNOWN, cannot cancel")

        # done waiting
        return
