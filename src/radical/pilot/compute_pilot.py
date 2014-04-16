#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.compute_pilot
   :platform: Unix
   :synopsis: Provides the interface for the ComputePilot class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time

from radical.pilot.states import *
from radical.pilot.exceptions import *

from radical.pilot.utils.logger import logger


# -----------------------------------------------------------------------------
#
class ComputePilot (object):
    """A ComputePilot represent a resource overlay on a local or remote
       resource.

    .. note:: A ComputePilot cannot be created directly. The factory method
              :meth:`radical.pilot.PilotManager.submit_pilots` has to be used instead.

                **Example**::

                      pm = radical.pilot.PilotManager(session=s)

                      pd = radical.pilot.ComputePilotDescription()
                      pd.resource = "localhost"
                      pd.cores    = 2
                      pd.runtime  = 5 # minutes

                      pilot = pm.submit_pilots(pd)
    """

    # -------------------------------------------------------------------------
    #
    def __init__(self):
        """Le constructeur. Not meant to be called directly.
        """
        # 'static' members
        self._uid = None
        self._description = None
        self._manager = None

        # Registered callback functions
        self._calback_wrappers = dict()

        # handle to the manager's worker
        self._worker = None

        # list of callback functions
        self._callback_list = []

    # -------------------------------------------------------------------------
    #
    def __del__(self):
        """Le destructeur.
        """
        if os.getenv("RADICALPILOT_GCDEBUG", None) is not None:
            logger.debug("__del__(): ComputePilot '%s'." % self._uid)

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def _create(pilot_manager_obj, pilot_description):
        """ PRIVATE: Create a new pilot.
        """
        # Create and return pilot object.
        pilot = ComputePilot()

        #pilot._uid = pilot_uid
        pilot._description = pilot_description
        pilot._manager = pilot_manager_obj

        # Pilots use the worker of their parent manager.
        pilot._worker = pilot._manager._worker

        #logger.info("Created new ComputePilot %s" % str(pilot))
        return pilot

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def _get(pilot_manager_obj, pilot_ids):
        """ PRIVATE: Get one or more pilot via their UIDs.
        """
        pilots_json = pilot_manager_obj._worker.get_compute_pilot_data(
            pilot_uids=pilot_ids)

        # create and return pilot objects
        pilots = []

        for p in pilots_json:
            pilot = ComputePilot()
            pilot._uid = str(p['_id'])
            pilot._description = p['description']
            pilot._manager = pilot_manager_obj

            pilot._worker = pilot._manager._worker

            logger.info("Reconnected to existing ComputePilot %s" % str(pilot))
            pilots.append(pilot)

        return pilots

    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the
           ComputePilot object.
        """
        obj_dict = {
            'uid':             self.uid,
            'state':           self.state,
            'log':             self.log,
            'sandbox':         self.sandbox,
            'resource':        self.resource,
            'submission_time': self.submission_time,
            'start_time':      self.start_time,
            'stop_time':       self.stop_time,
            'resource_detail': self.resource_detail
        }
        return obj_dict

    # -------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the ComputePilot object.
        """
        return str(self.as_dict())

    # -------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the Pilot's unique identifier.

        The uid identifies the Pilot within the :class:`PilotManager` and
        can be used to retrieve an existing Pilot.

        **Returns:**
            * A unique identifier (string).
        """
        return self._uid

    # -------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the pilot description the pilot was started with.
        """
        return self._description

    # -------------------------------------------------------------------------
    #
    @property
    def sandbox(self):
        """Returns the Pilot's 'sandbox' / working directory url.

        **Returns:**
            * A URL string.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['sandbox']

    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['state']

    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the log of the pilot.

        This 
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['log']

    # -------------------------------------------------------------------------
    #
    @property
    def resource_detail(self):
        """Returns the names of the nodes managed by the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        resource_details = {
            'nodes':          pilot_json['nodes'],
            'cores_per_node': pilot_json['cores_per_node']
        }
        return resource_details

    # -------------------------------------------------------------------------
    #
    @property
    def pilot_manager(self):
        """ Returns the pilot manager object for this pilot.
        """
        return self._manager

    # -------------------------------------------------------------------------
    #
    @property
    def unit_managers(self):
        """ Returns the unit manager object UIDs for this pilot.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        raise exceptions.radical.pilotException("Not Implemented")

    # -------------------------------------------------------------------------
    #
    @property
    def units(self):
        """ Returns the units scheduled for this pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        raise exceptions.radical.pilotException("Not Implemented")

    # -------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the pilot was submitted.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['submitted']

    # -------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the pilot was started on the backend.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['started']

    # -------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the pilot was stopped.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['finished']

    # -------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """ Returns the resource.
        """
        if not self._uid:
            raise exceptions.IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_uids=self.uid)
        return pilot_json['description']['Resource']

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func):
        """Registers a callback function that is triggered every time the
        ComputePilot's state changes.

        All callback functions need to have the same signature::

            def callback_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """
        self._worker.register_pilot_callback(self, callback_func)

    # -------------------------------------------------------------------------
    #
    def wait(self, state=[DONE, FAILED, CANCELED],
             timeout=None):
        """Returns when the pilot reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of strings`]
              The state(s) that Pilot has to reach in order for the
              call to return.

              By default `wait` waits for the Pilot to reach
              a **terminal** state, which can be one of the following:

              * :data:`radical.pilot.states.DONE`
              * :data:`radical.pilot.states.FAILED`
              * :data:`radical.pilot.states.CANCELED`

            * **timeout** [`float`]
              Optional timeout in seconds before the call returns regardless
              whether the Pilot has reached the desired state or not.
              The default value **None** never times out.

        **Raises:**

            * :class:`radical.pilot.exceptions.radical.pilotException` if the state of
              the pilot cannot be determined.
        """
        # Check if this instance is valid
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

            if (timeout is not None) and (timeout <= (time.time() - start_wait)):
                break

        # done waiting
        return new_state

    # -------------------------------------------------------------------------
    #
    def cancel(self):
        """Sends sends a termination request to the pilot.

        **Raises:**

            * :class:`radical.pilot.radical.pilotException` if the termination
              request cannot be fulfilled.
        """
        # Check if this instance is valid
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid instance.")

        if self.state in [DONE, FAILED, CANCELED]:
            # nothing to do as we are already in a terminal state
            return

        if self.state == UNKNOWN:
            msg = "Invalid pilot state: '%s'" % UNKNOWN
            raise exceptions.BadParameter(msg=msg)

        # now we can send a 'cancel' command to the pilot.
        self._manager.cancel_pilots(self.uid)
