"""
.. module:: sinon.compute_pilot
   :platform: Unix
   :synopsis: Implementation of the ComputePilot class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import sinon.api.states     as states
import sinon.api.attributes as attributes
import sinon.api.exceptions as exceptions

import time

# ------------------------------------------------------------------------------
# Attribute keys
UID               = 'UID'
DESCRIPTION       = 'Description'

STATE             = 'State'
STATE_DETAILS     = 'StateDetails'

SUBMISSION_TIME   = 'SubmissionTime'
START_TIME        = 'StartTime'
STOP_TIME         = 'StopTime'

PILOT_MANAGER     = 'PilotManager'
UNIT_MANAGERS     = 'UnitManagers'
UNITS             = 'Units'

# ------------------------------------------------------------------------------
#
class ComputePilot (attributes.Attributes) :

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

        # initialize attributes
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

        # The state detail a.k.a. 'log' attributesribute 
        self._attributes_register(STATE_DETAILS, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(STATE_DETAILS, self._get_state_detail_priv)

        # The units assigned to this pilot
        self._attributes_register(UNITS, None,  attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_set_getter(UNITS, self._get_units_priv)

        # The unit managers this pilot is attached to
        self._attributes_register(UNIT_MANAGERS, None,  attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_set_getter(UNIT_MANAGERS, self._get_unit_managers_priv)

        # The pilot manager this pilot is attached to
        self._attributes_register(PILOT_MANAGER, None,  attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(PILOT_MANAGER, self._get_pilot_manager_priv)

        # The submission time
        self._attributes_register(SUBMISSION_TIME, None,  attributes.STRING, attributes.VECTOR, attributes.READONLY)
        self._attributes_set_getter(SUBMISSION_TIME, self._get_submission_time_priv)

        # The start time
        self._attributes_register(START_TIME, None,  attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(START_TIME, self._get_start_time_priv)

        # The stop time
        self._attributes_register(STOP_TIME, None,  attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(STOP_TIME, self._get_stop_time_priv)

    # --------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        pass

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _create (pilot_manager_obj, pilot_id, pilot_description):
        """ PRIVATE: Create a new pilot.
        """
        # create and return pilot object
        pilot = ComputePilot()

        pilot._uid = pilot_id
        pilot._description = pilot_description
        pilot._manager     = pilot_manager_obj

        pilot._DB = pilot._manager._DB

        return pilot

    # --------------------------------------------------------------------------
    #
    @staticmethod 
    def _get (pilot_manager_obj, pilot_ids) :
        """ PRIVATE: Get one or more pilot via their UIDs.
        """
        # create database entry
        pilots_json = pilot_manager_obj._DB.get_pilots(
            pilot_manager_id=pilot_manager_obj.uid, pilot_ids=pilot_ids)
        # create and return pilot objects
        pilots = []

        for p in pilots_json:
            pilot = ComputePilot()
            pilot._uid = str(p['_id'])
            pilot._description = p['description']
            pilot._manager = pilot_manager_obj

            pilot._DB = pilot._manager._DB
        
            pilots.append(pilot)

        return pilots

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
            raise exceptions.SinonException(msg="Invalid object instance.")

        return self._uid

    # --------------------------------------------------------------------------
    #
    def _get_description_priv(self):
        """PRIVATE: Returns the pilot description the pilot was started with.
        """
        if not self._uid:
            raise exceptions.SinonException(msg="Invalid object instance.")

        return self._description

    # --------------------------------------------------------------------------
    #
    def _get_state_priv(self):
        """PRIVATE: Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.SinonException(msg="Invalid object instance.")

        # state is dynamic and changes over the lifetime of a pilot, hence we 
        # need to make a call to the database layer
        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
            pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.SinonException(msg=msg)

        return pilots_json[0]['info']['state']

    # --------------------------------------------------------------------------
    #
    def _get_state_detail_priv(self):
        """PRIVATE: Returns the current state of the pilot.

        This 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        # state detail is dynamic and changes over the  lifetime of a pilot,
        # hence we need to make a call to the  database layer.
        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.SinonException(msg=msg)

        return pilots_json[0]['info']['log']

    # --------------------------------------------------------------------------
    #
    def _get_pilot_manager_priv(self):
        """ Returns the pilot manager object for this pilot.
        """
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        return self._manager

    # --------------------------------------------------------------------------
    #
    def _get_unit_managers_priv(self):
        """ Returns the pilot manager object for this pilot.
        """
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        raise exceptions.SinonException("Not Implemented")


    # --------------------------------------------------------------------------
    #
    def _get_units_priv(self):
        """ Returns the units scheduled for this pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        raise exceptions.SinonException("Not Implemented")

    # --------------------------------------------------------------------------
    #
    def _get_submission_time_priv(self):
        """ Returns the time the pilot was submitted. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.SinonException(msg=msg)

        return pilots_json[0]['info']['submitted']

    # --------------------------------------------------------------------------
    #
    def _get_start_time_priv(self):
        """ Returns the time the pilot was started on the backend. 
        """
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.SinonException(msg=msg)

        return pilots_json[0]['info']['started']

    # --------------------------------------------------------------------------
    #
    def _get_stop_time_priv(self):
        """ Returns the time the pilot was stopped. 
        """
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.SinonException(msg=msg)

        return pilots_json[0]['info']['finished']

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns string representation of this pilot.
        """
        return str({'type': 'ComputePilot', 'id': self.uid})

    # --------------------------------------------------------------------------
    #
    def wait (self, state=[states.DONE, states.FAILED, states.CANCELED], timeout=None):
        """Returns when the pilot reaches a specific state or 
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that Pilot has to reach in order for the 
              call to return. 

              By default `wait` waits for the Pilot to reach 
              a **terminal** state, which can be one of the following:

              * :data:`sinon.states.DONE`
              * :data:`sinon.states.FAILED`
              * :data:`sinon.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless 
              whether the Pilot has reached the desired state or not. 
              The default value **None** never times out.

        **Raises:**

            * :class:`sinon.exceptions.SinonException` if the state of the 
              pilot cannot be determined. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid object instance.")

        if not isinstance (state, list):
            state = [state]

        start_wait = time.time ()
        # the self.state property pulls the state from the back end.
        while self.state not in state:
            time.sleep (1)

            if  (None != timeout) and (timeout <= (time.time () - start_wait)) :
                break

        # done waiting
        return

    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """Sends sends a termination request to the pilot.

        **Raises:**

            * :class:`sinon.exceptions.SinonException if the termination 
              request cannot be fulfilled. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException(msg="Invalid object instance.")

        if self.state in [states.DONE, states.FAILED, states.CANCELED]:
            # nothing to do as we are already in a terminal state
            return

        if self.state == states.UNKNOWN:
            msg = "Invalid pilot state: '%s'" % states.UNKNOWN
            raise exceptions.SinonException(msg=msg)

        # now we can send a 'cancel' command to the pilot.
        self._DB.signal_pilots(pilot_manager_id=self._manager.uid, 
            pilot_ids=self.uid, cmd="CANCEL")
