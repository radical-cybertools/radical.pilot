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
import copy
import time

import radical.utils as ru

from .states             import *
from .logentry           import *
from .exceptions         import *
from .utils              import logger
from .db.database        import COMMAND_CANCEL_COMPUTE_UNIT
from .staging_directives import expand_staging_directive

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
        self._name = None
        self._description = None
        self._manager = None

        # handle to the manager's worker
        self._worker = None

        if os.getenv("RADICAL_PILOT_GCDEBUG", None) is not None:
            logger.debug("GCDEBUG __init__(): ComputeUnit [object id: %s]." % id(self))

    #--------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        if os.getenv("RADICAL_PILOT_GCDEBUG", None) is not None:
            logger.debug("GCDEBUG __del__(): ComputeUnit [object id: %s]." % id(self))


    #--------------------------------------------------------------------------
    #
    def __repr__(self):

        return "%s (%-15s: %s %s) (%s)" % (self.uid, self.state,
                                           self.description.executable, 
                                           self.description.arguments, 
                                           id(self))


    # -------------------------------------------------------------------------
    #
    @staticmethod
    def create(unit_manager_obj, unit_description, local_state):
        """ PRIVATE: Create a new compute unit.
        """
        # create and return pilot object
        computeunit = ComputeUnit()

        # Make a copy of the UD to work on without side-effects.
        ud_copy = copy.deepcopy(unit_description)

        # sanity check on description
        if  (not 'executable' in unit_description or \
             not unit_description['executable']   )  and \
            (not 'kernel'     in unit_description or \
             not unit_description['kernel']       )  :
            raise PilotException ("ComputeUnitDescription needs an executable or application kernel")

        # Validate number of cores
        if not unit_description.cores > 0:
            raise BadParameter("Can't run a Compute Unit with %d cores." % unit_description.cores)

        # If staging directives exist, try to expand them
        if  ud_copy.input_staging:
            ud_copy.input_staging = expand_staging_directive(ud_copy.input_staging)

        if  ud_copy.output_staging:
            ud_copy.output_staging = expand_staging_directive(ud_copy.output_staging)

        computeunit._description = ud_copy
        computeunit._manager     = unit_manager_obj
        computeunit._session     = unit_manager_obj._session
        computeunit._worker      = unit_manager_obj._worker
        computeunit._uid         = ru.generate_id('unit.%(counter)06d', ru.ID_CUSTOM)
        computeunit._name        = unit_description['name']
        computeunit._local_state = local_state

        computeunit._session.prof.prof('advance', msg=NEW, uid=computeunit._uid, state=NEW)

        return computeunit

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def _get(unit_manager_obj, unit_ids):
        """ PRIVATE: Get one or more Compute Units via their UIDs.
        """
        units_json = unit_manager_obj._session._dbs.get_compute_units(
            unit_manager_id=unit_manager_obj.uid,
            unit_ids=unit_ids
        )
        # create and return unit objects
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
            'name':              self.name,
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
            return None

        return str(self.as_dict())

    # -------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the unit's unique identifier.

        The uid identifies the ComputeUnit within a :class:`UnitManager` and
        can be used to retrieve an existing ComputeUnit.

        **Returns:**
            * A unique identifier (string).
        """
        # uid is static and doesn't change over the lifetime
        # of a unit, hence it can be stored in a member var.
        return self._uid

    # -------------------------------------------------------------------------
    #
    @property
    def name(self):
        """Returns the unit's application specified name.

        **Returns:**
            * A name (string).
        """
        # name is static and doesn't change over the lifetime
        # of a unit, hence it can be stored in a member var.
        return self._name

    # -------------------------------------------------------------------------
    #
    @property
    def working_directory(self):
        """Returns the full working directory URL of this ComputeUnit.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['sandbox']

    # -------------------------------------------------------------------------
    #
    @property
    def pilot_id(self):
        """Returns the pilot_id of this ComputeUnit.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json.get ('pilot', None)

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
            return None

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
            return None

        return self._worker.get_compute_unit_stderr(self.uid)

    # -------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the ComputeUnitDescription the ComputeUnit was started with.
        """
        # description is static and doesn't change over the lifetime
        # of a unit, hence it is stored as a member var.
        return self._description

    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the ComputeUnit.
        """
        if not self._uid:
            return None

        # try to get state from worker.  If that fails, return local state.
        # NOTE AM: why?  Isn't that an error which should not occur?
        try :
            cu_json = self._worker.get_compute_unit_data(self.uid)
            return cu_json['state']
        except Exception as e :
            return self._local_state

    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """Returns the complete state history of the ComputeUnit.
        """
        if not self._uid:
            return None

        states = []

        cu_json = self._worker.get_compute_unit_data(self.uid)
        for state in cu_json['statehistory']:
            states.append(State(state=state["state"], timestamp=state["timestamp"]))

        return states

    # -------------------------------------------------------------------------
    #
    @property
    def callback_history(self):
        """Returns the complete callback history of the ComputeUnit.
        """
        if not self._uid:
            return None

        callbacks = []

        cu_json = self._worker.get_compute_unit_data(self.uid)
        if 'callbackhostory' in cu_json :
            for callback in cu_json['callbackhistory']:
                callbacks.append(State(state=callback["state"], timestamp=callback["timestamp"]))

        return callbacks

    # -------------------------------------------------------------------------
    #
    @property
    def exit_code(self):
        """Returns the exit code of the ComputeUnit.

        If this property is queried before the ComputeUnit has reached
        'DONE' or 'FAILED' state it will return None.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['exit_code']

    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the logs of the ComputeUnit.
        """
        if not self._uid:
            return None

        logs = []

        cu_json = self._worker.get_compute_unit_data(self.uid)
        for log in cu_json['log']:
            logs.append(Logentry.from_dict (log))

        return logs

    # -------------------------------------------------------------------------
    #
    @property
    def execution_details(self):
        """Returns the exeuction location(s) of the ComputeUnit.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json

    # -------------------------------------------------------------------------
    #
    @property
    def execution_locations(self):
        """Returns the exeuction location(s) of the ComputeUnit.
           This is just an alias for execution_details.
        """
        return self.execution_details['exec_locs']

    # -------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the ComputeUnit was submitted.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['submitted']

    # -------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the ComputeUnit was started on the backend.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['started']

    # -------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the ComputeUnit was stopped.
        """
        if not self._uid:
            return None

        cu_json = self._worker.get_compute_unit_data(self.uid)
        return cu_json['finished']

    # -------------------------------------------------------------------------
    #
    def register_callback(self, cb_func, cb_data=None):
        """Registers a callback function that is triggered every time the
        ComputeUnit's state changes.

        All callback functions need to have the same signature::

            def cb_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """
        self._worker.register_unit_callback(self, cb_func, cb_data)

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
            raise IncorrectState("Invalid instance.")

        if not isinstance(state, list):
            state = [state]

        start_wait = time.time()
        # the self.state property pulls the state from the back end.
        new_state = self.state
        while new_state not in state:
            time.sleep(0.1)

            new_state = self.state
            # logger.debug(
            #     "Compute unit %s in state %s" % (self._uid, new_state))

            if(None != timeout) and (timeout <= (time.time() - start_wait)):
                break

        # done waiting -- return the state
        return new_state

    # -------------------------------------------------------------------------
    #
    def cancel(self):
        """Cancel the ComputeUnit.

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        
        # Check if this instance is valid
        if not self._uid:
            raise BadParameter("Invalid Compute Unit instance.")

        cu_json = self._worker.get_compute_unit_data(self.uid)
        pilot_uid = cu_json['pilot']

        if self.state in [DONE, FAILED, CANCELED]:
            # nothing to do
            logger.debug("Compute unit %s has state %s, can't cancel any longer." % (self._uid, self.state))

        elif self.state in [NEW, UNSCHEDULED, PENDING_INPUT_STAGING]:
            logger.debug("Compute unit %s has state %s, going to prevent from starting." % (self._uid, self.state))
            self._manager._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state == STAGING_INPUT:
            logger.debug("Compute unit %s has state %s, will cancel the transfer." % (self._uid, self.state))
            self._manager._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state in [EXECUTING_PENDING, SCHEDULING]:
            logger.debug("Compute unit %s has state %s, will abort start-up." % (self._uid, self.state))
            self._manager._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state == EXECUTING:
            logger.debug("Compute unit %s has state %s, will terminate the task." % (self._uid, self.state))
            self._manager._session._dbs.send_command_to_pilot(cmd=COMMAND_CANCEL_COMPUTE_UNIT, arg=self.uid, pilot_ids=pilot_uid)

        elif self.state == PENDING_OUTPUT_STAGING:
            logger.debug("Compute unit %s has state %s, will abort the transfer." % (self._uid, self.state))
            self._manager._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        elif self.state == STAGING_OUTPUT:
            logger.debug("Compute unit %s has state %s, will cancel the transfer." % (self._uid, self.state))
            self._manager._session._dbs.set_compute_unit_state(self._uid, CANCELED, ["Received Cancel"])

        else:
            raise IncorrectState("Unknown Compute Unit state: %s, cannot cancel" % self.state)

        self._session.prof.prof('advance', msg=CANCELED, uid=self._uid, state=CANCELED)

        # done canceling
        return

