"""
.. module:: sinon.compute_unit
   :platform: Unix
   :synopsis: Implementation of the ComputeUnit class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import sinon.api.states      as states
import sinon.api.attributes  as attributes
import sinon.api.exceptions  as exceptions
from sinon.utils.logger      import logger

import time

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
class ComputeUnit(attributes.Attributes):

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
        self._DB = None

        attributes.Attributes.__init__(self)

        # set attributesribute interface properties
        self._attributes_extensible(False)
        self._attributes_camelcasing(True)

        # The UID attributesribute
        self._attributes_register(UID, self._uid, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(UID, self._get_uid_priv)

        # The description attributesribute
        self._attributes_register(DESCRIPTION, self._description, attributes.ANY, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(DESCRIPTION, self._get_description_priv)

        # The state attributesribute
        self._attributes_register(STATE, states.UNKNOWN, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(STATE, self._get_state_priv)

        # The state detail a.k.a. 'log' attribute 
        self._attributes_register(STATE_DETAILS, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(STATE_DETAILS, self._get_state_details_priv)

        # The execution details attribute 
        self._attributes_register(EXECUTION_DETAILS, None, attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_set_getter(EXECUTION_DETAILS, self._get_execution_details_priv)

        # The submission time
        self._attributes_register(SUBMISSION_TIME, None,  attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(SUBMISSION_TIME, self._get_submission_time_priv)

        # The start time
        self._attributes_register(START_TIME, None,  attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(START_TIME, self._get_start_time_priv)

        # The stop time
        self._attributes_register(STOP_TIME, None,  attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(STOP_TIME, self._get_stop_time_priv)


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

        computeunit._DB          = unit_manager_obj._DB

        return computeunit

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _get (unit_manager_obj, unit_ids) :
        """ PRIVATE: Get one or more pilot via their UIDs.
        """
        # create database entry
        units_json = unit_manager_obj._DB.get_workunits(
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

            computeunit._DB = unit_manager_obj._DB
        
            computeunits.append(computeunit)

        return computeunits

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns information about the comnpute unit as a Python dictionary.
        """
        info_dict = {
            'type'            : 'ComputeUnit', 
            'id'              : self._get_uid_priv(), 
            'state'           : self._get_state_priv(),
 #           'submission_time' : self._get_submission_time_priv(), 
 #           'start_time'      : self._get_start_time_priv(), 
 #           'stop_time'       : self._get_stop_time_priv()
        }
        return info_dict

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the compute unit.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        return str(self.as_dict())


    # --------------------------------------------------------------------------
    #
    def _get_uid_priv(self):
        """PRIVATE: Returns the Pilot's unique identifier.

        The uid identifies the Pilot within the :class:`PilotManager` and 
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
    def _get_description_priv(self):
        """PRIVATE: Returns the pilot description the pilot was started with.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # description is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._description

    # --------------------------------------------------------------------------
    #
    def _get_state_priv(self):
        """PRIVATE: Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # state is dynamic and changes over the  lifetime of a pilot, hence we
        # need to make a call to the  database layer (db layer might cache
        # this call).
        workunit_json = self._DB.get_workunit_states(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]
        
    # --------------------------------------------------------------------------
    #
    def _get_state_details_priv(self):
        """PRIVATE: Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._DB.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
            )

        # state detail is oviously dynamic and changes over the 
        # lifetime of a pilot, hence we need to make a call to the 
        # database layer (db layer might cache this call).
        pass

    # --------------------------------------------------------------------------
    #
    def _get_execution_details_priv(self):
        """PRIVATE: Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._DB.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['exec_locs']

    # --------------------------------------------------------------------------
    #
    def _get_submission_time_priv(self):
        """ Returns the time the compute unit was submitted. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._DB.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['submitted']


    # --------------------------------------------------------------------------
    #
    def _get_start_time_priv(self):
        """ Returns the time the compute unit was started on the backend. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._DB.get_workunits(
            workunit_manager_id=self._manager.uid, 
            workunit_ids=[self.uid]
        )

        return workunit_json[0]['info']['started']


    # --------------------------------------------------------------------------
    #
    def _get_stop_time_priv(self):
        """ Returns the time the compute unit was stopped. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        workunit_json = self._DB.get_workunits(
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

              * :data:`sinon.states.DONE`
              * :data:`sinon.states.FAILED`
              * :data:`sinon.states.CANCELED`

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

            * :class:`sinon.SinonException
        """
        # Check if this instance is valid
        if not self._uid:
            raise excpetions.SinonException("Invalid Compute Unit instance.")

        if self.state in [states.DONE, states.FAILED, states.CANCELED]:
            # nothing to do
            return

        if self.state in [states.UNKNOWN] :
            raise excpetions.SinonException("Compute Unit state is UNKNOWN, cannot cancel")

        # done waiting
        return


