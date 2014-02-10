#pylint: disable=C0301, C0103, W0212

"""
.. module:: sagapilot.compute_unit
   :platform: Unix
   :synopsis: Implementation of the ComputeUnit class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import time

import sagapilot.states        as states
import sagapilot.exceptions    as exceptions
from   sagapilot.utils.logger  import logger

# ------------------------------------------------------------------------------
# Attribute keys
UID               = 'UID'
DESCRIPTION       = 'Description'
STATE             = 'State'
STATE_DETAILS     = 'StateDetails'
EXECUTION_DETAILS = 'ExecutionDetails'

SUBMISSION_TIME   = 'SubmissionTime'
START_TIME        = 'StartTime'
STOP_TIME         = 'StopTime'

UNIT_MANAGER      = 'UnitManagers'
PILOT             = 'Pilot'

# ------------------------------------------------------------------------------
#
class ComputeUnit(object): #attributes.Attributes):
    """TODO: document me!
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self):
        """ Le constructeur. Not meant to be called directly.
        """
        # 'static' members
        self._uid = None
        self._description = None
        self._manager = None

        # database handle
        self._db = None

    #---------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        if os.getenv("SAGAPILOT_GCDEBUG", None) is not None:
            logger.debug("__del__(): ComputeUnit '%s'." % self._uid )

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _create (unit_manager_obj, unit_id, unit_description):
        """ PRIVATE: Create a new compute unit.
        """
        # create and return pilot object
        computeunit = ComputeUnit()

        computeunit._uid         = unit_id
        computeunit._description = unit_description
        computeunit._manager     = unit_manager_obj

        computeunit._db          = unit_manager_obj._session._dbs

        return computeunit

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _get (unit_manager_obj, unit_ids) :
        """ PRIVATE: Get one or more pilot via their UIDs.
        """
        # create database entry
        units_json = unit_manager_obj._db.get_workunits(
            workunit_manager_id=unit_manager_obj.uid, 
            workunit_ids=unit_ids
        )
        # create and return pilot objects
        computeunits = []

        for u in units_json:
            computeunit = ComputeUnit()
            computeunit._uid = str(u['_id'])
            computeunit._description = u['description']
            computeunit._manager = unit_manager_obj

            computeunit._db = unit_manager_obj._session._dbs
        
            computeunits.append(computeunit)

        return computeunits

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        obj_dict = {
            'uid'               : self.uid,
            'state'             : self.state,
            'state_details'     : self.state_details,
            'execution_details' : self.state_details,
            'submission_time'   : self.submission_time, 
            'start_time'        : self.start_time, 
            'stop_time'         : self.stop_time
        }
        return obj_dict

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the object.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the Pilot's unique identifier.

        The uid identifies the ComputePilot within a :class:`PilotManager` and 
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # uid is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._uid

    # --------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """Returns a snapshot of the executable's STDOUT stream.

        .. warning: This can become very inefficient for lare data volumes.
        """
        stdout_str = self._db.get_workunit_stdout(workunit_uid=self.uid)
        return stdout_str

    # --------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """Returns a snapshot of the executable's STDERR stream.

        .. warning: This can become very inefficient for lare data volumes.
        """
        stderr_str = self._db.get_workunit_stderr(workunit_uid=self.uid)
        return stderr_str



    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the pilot description the pilot was started with.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # description is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._description

    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # state is dynamic and changes over the  lifetime of a pilot, hence we
        # need to make a call to the  database layer (db layer might cache
        # this call).
        workunit_json = self._db.get_workunit_states(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]
        
    # --------------------------------------------------------------------------
    #
    @property
    def state_details(self):
        """Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._db.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
            )

        # TODO: implement me 

        return "unknown"

    # --------------------------------------------------------------------------
    #
    @property
    def execution_details(self):
        """Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._db.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['exec_locs']

    # --------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the compute unit was submitted. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._db.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['submitted']

    # --------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the compute unit was started on the backend. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._db.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['started']

    # --------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the compute unit was stopped. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._db.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['finished']

    # --------------------------------------------------------------------------
    #
    def wait(self, state=[states.DONE, states.FAILED, states.CANCELED], timeout=None):
        """Returns when the compute unit reaches a specific state or 
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that compute unit has to reach in order for the 
              call to return. 

              By default `wait` waits for the compute unit to reach 
              a **terminal** state, which can be one of the following:

              * :data:`sagapilot.states.DONE`
              * :data:`sagapilot.states.FAILED`
              * :data:`sagapilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless 
              whether the compute unit has reached the desired state or not. 
              The default value **None** never times out.

        **Raises:**
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        if not isinstance (state, list):
            state = [state]

        start_wait = time.time ()
        # the self.state property pulls the state from the back end.
        new_state = self.state
        while new_state not in state:
            time.sleep (1)

            new_state = self.state
            logger.debug("Compute unit %s in state %s" % (self._uid, new_state))

            if  (None != timeout) and (timeout <= (time.time () - start_wait)):
                break

        # done waiting
        return

    # --------------------------------------------------------------------------
    #
    def cancel (self):
        """Terminates the compute unit.

        **Raises:**

            * :class:`sagapilot.SagapilotException`
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SagapilotException("Invalid Compute Unit instance.")

        if self.state in [states.DONE, states.FAILED, states.CANCELED]:
            # nothing to do
            return

        if self.state in [states.UNKNOWN] :
            raise exceptions.SagapilotException("Compute Unit state is UNKNOWN, cannot cancel")

        # done waiting
        return
