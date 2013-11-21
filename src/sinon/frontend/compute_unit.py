"""
.. module:: sinon.compute_unit
   :platform: Unix
   :synopsis: Implementation of the ComputeUnit class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import sinon.frontend.states as states
import sinon.frontend.attributes as attributes
import sinon.frontend.excetions as exceptions

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
    def _get_uid_priv(self):
        """PRIVATE: Returns the Pilot's unique identifier.

        The uid identifies the Pilot within the :class:`PilotManager` and 
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid Compute Unit instance.")

        # uid is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._uid

    # --------------------------------------------------------------------------
    #
    def _get_description_priv(self):
        """PRIVATE: Returns the pilot description the pilot was started with.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid Compute Unit instance.")

        # description is static and doesn't change over the lifetime 
        # of a pilot, hence it can be stored in a member var.
        return self._description

    # --------------------------------------------------------------------------
    #
    def _get_state_priv(self):
        """PRIVATE: Returns the current state of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid Compute Unit instance.")

        # state is oviously dynamic and changes over the 
        # lifetime of a pilot, hence we need to make a call to the 
        # database layer (db layer might cache this call).
        pass

    # --------------------------------------------------------------------------
    #
    def _get_state_detail_priv(self):
        """PRIVATE: Returns the current state of the pilot.

        This 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.SinonException("Invalid Compute Unit instance.")

        # state detail is oviously dynamic and changes over the 
        # lifetime of a pilot, hence we need to make a call to the 
        # database layer (db layer might cache this call).
        pass

    # --------------------------------------------------------------------------
    #
    def _get_submission_time_priv(self):
        """ Returns the time the compute unit was submitted. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise excpetions.SinonException("Invalid Compute Unit instance.")

        pilots_json = self._db.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])
        return pilots_json[0]['info']['submitted']


    # --------------------------------------------------------------------------
    #
    def _get_start_time_priv(self):
        """ Returns the time the compute unit was started on the backend. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise excpetions.SinonException("Invalid Compute Unit instance.")

        raise excpetions.SinonException("Not Implemented")

    # --------------------------------------------------------------------------
    #
    def _get_stop_time_priv(self):
        """ Returns the time the compute unit was stopped. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise excpetions.SinonException("Invalid Compute Unit instance.")

        raise excpetions.SinonException("Not Implemented")

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
        # Check if this instance is valid
        if not self._uid:
            raise excpetions.SinonException("Invalid Compute Unit instance.")

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

        # now we can send a 'cancel' command to the pilot
        # through the database layer. 
        pass


