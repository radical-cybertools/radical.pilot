"""
.. module:: sagapilot.compute_pilot
   :platform: Unix
   :synopsis: Implementation of the ComputePilot class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sagapilot.states     as states
import sagapilot.exceptions as exceptions

from sagapilot.utils.logger     import logger

import time

# ------------------------------------------------------------------------------
# Attribute keys
UID               = 'UID'
DESCRIPTION       = 'Description'

STATE             = 'State'
STATE_DETAILS     = 'StateDetails'
RESOURCE_DETAILS  = 'ResourceDetails'

SUBMISSION_TIME   = 'SubmissionTime'
START_TIME        = 'StartTime'
STOP_TIME         = 'StopTime'

PILOT_MANAGER     = 'PilotManager'
UNIT_MANAGERS     = 'UnitManagers'
UNITS             = 'Units'

# ------------------------------------------------------------------------------
#
class ComputePilot (object):
    """A ComputePilot represent a resource overlay on a local or remote
       resource. 
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self):
        """Le constructeur. Not meant to be called directly.
        """
        # 'static' members
        self._uid = None
        self._description = None
        self._manager = None

        # database handle
        self._DB = None

    # --------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        logger.debug("__del__(): ComputePilot '%s'." % self._uid )


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

        logger.info("Created new ComputePilot %s" % str(pilot))

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
        
            logger.info("Reconnected to existing ComputePilot %s" % str(pilot))
            pilots.append(pilot)

        return pilots

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the 
           ComputePilot object.
        """
        obj_dict = {
            'uid'              : self.uid, 
            'state'            : self.state,
            'resource'         : self.resource,
            'submission_time'  : self.submission_time, 
            'start_time'       : self.start_time, 
            'stop_time'        : self.stop_time
        }
        return obj_dict

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the ComputePilot object.
        """
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the Pilot's unique identifier.

        The uid identifies the Pilot within the :class:`PilotManager` and 
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        return self._uid

    # --------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the pilot description the pilot was started with.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        return self._description

    # --------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        # state is dynamic and changes over the lifetime of a pilot, hence we 
        # need to make a call to the database layer
        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
            pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.SagapilotException(msg=msg)

        return pilots_json[0]['info']['state']

    # --------------------------------------------------------------------------
    #
    @property 
    def state_detail(self):
        """Returns the current state of the pilot.

        This 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # state detail is dynamic and changes over the  lifetime of a pilot,
        # hence we need to make a call to the  database layer.
        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.BadParameter(msg=msg)

        return pilots_json[0]['info']['log']


    # --------------------------------------------------------------------------
    #
    @property
    def resource_detail(self):
        """Returns the resource details of the pilot.

        This 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        # state detail is dynamic and changes over the  lifetime of a pilot,
        # hence we need to make a call to the  database layer.
        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.BadParameter(msg=msg)

        resource_details = {
            'nodes'          : pilots_json[0]['info']['nodes'],
            'cores_per_node' : pilots_json[0]['info']['cores_per_node']
        }

        return resource_details

    # --------------------------------------------------------------------------
    #
    @property
    def pilot_manager(self):
        """ Returns the pilot manager object for this pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        return self._manager

    # --------------------------------------------------------------------------
    #
    @property
    def unit_managers(self):
        """ Returns the pilot manager object for this pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        raise exceptions.SagapilotException("Not Implemented")


    # --------------------------------------------------------------------------
    #
    @property
    def units(self):
        """ Returns the units scheduled for this pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        raise exceptions.SagapilotException("Not Implemented")

    # --------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the pilot was submitted. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.BadParameter(msg=msg)

        return pilots_json[0]['info']['submitted']

    # --------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the pilot was started on the backend. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.BadParameter(msg=msg)

        return pilots_json[0]['info']['started']

    # --------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the pilot was stopped. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.BadParameter(msg=msg)

        return pilots_json[0]['info']['finished']

    # --------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """ Returns the resource. 
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilots_json = self._DB.get_pilots(pilot_manager_id=self._manager.uid, 
                                          pilot_ids=[self.uid])

        # make sure the result makes sense
        if len(pilots_json) != 1: 
            msg = "Couldn't find pilot with UID '%s'" % self.uid
            raise exceptions.BadParameter(msg=msg)

        return pilots_json[0]['description']['Resource']



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

              * :data:`sagapilot.states.DONE`
              * :data:`sagapilot.states.FAILED`
              * :data:`sagapilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless 
              whether the Pilot has reached the desired state or not. 
              The default value **None** never times out.

        **Raises:**

            * :class:`sagapilot.exceptions.SagapilotException` if the state of the 
              pilot cannot be determined. 
        """
        # Check if this instance is valid
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

            if  (timeout is not None) and (timeout <= (time.time() - start_wait)):
                break

        # done waiting
        return new_state

    # --------------------------------------------------------------------------
    #
    def cancel(self):
        """Sends sends a termination request to the pilot.

        **Raises:**

            * :class:`sagapilot.SagapilotException` if the termination 
              request cannot be fulfilled. 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        if self.state in [states.DONE, states.FAILED, states.CANCELED]:
            # nothing to do as we are already in a terminal state
            return

        if self.state == states.UNKNOWN:
            msg = "Invalid pilot state: '%s'" % states.UNKNOWN
            raise exceptions.BadParameter(msg=msg)

        # now we can send a 'cancel' command to the pilot.
        self._manager.cancel_pilots(self.uid)
