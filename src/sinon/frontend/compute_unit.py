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

PILOT_MANAGER     = 'PilotManager'
UNITS             = 'Units'
UNIT_MANAGERS     = 'UnitManagers'

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
            raise exceptions.SinonException("Invalid Pilot instance.")

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
            raise exceptions.SinonException("Invalid Pilot instance.")

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
            raise exceptions.SinonException("Invalid Pilot instance.")

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
            raise exceptions.SinonException("Invalid Pilot instance.")

        # state detail is oviously dynamic and changes over the 
        # lifetime of a pilot, hence we need to make a call to the 
        # database layer (db layer might cache this call).
        pass




