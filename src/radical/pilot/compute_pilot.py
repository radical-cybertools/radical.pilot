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
import saga

from radical.pilot.states import *
from radical.pilot.logentry import *
from radical.pilot.exceptions import *

from radical.pilot.utils.logger import logger

from radical.pilot.staging_directives import TRANSFER, COPY, LINK, MOVE, \
    STAGING_AREA, expand_staging_directive

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
                      pd.resource = "local.localhost"
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
            pilot_ids=pilot_ids)

        # create and return pilot objects
        pilots = []

        for p in pilots_json:
            pilot = ComputePilot()
            pilot._uid = str(p['_id'])
            pilot._description = p['description']
            pilot._manager = pilot_manager_obj

            pilot._worker = pilot._manager._worker

            logger.debug("Reconnected to existing ComputePilot %s" % str(pilot))
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
            'stdout':          self.stdout,
            'stderr':          self.stderr,
            'logfile':         self.logfile,
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
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['sandbox']

    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the pilot.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['state']

    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """Returns the complete state history of the pilot.
        """
        if not self._uid:
            return None

        states = []

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        for state in pilot_json['statehistory']:
            states.append(State(state=state["state"], timestamp=state["timestamp"]))

        return states

    # -------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        """Returns the stdout of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json.get ('stdout')

    # -------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        """Returns the stderr of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json.get ('stderr')

    # -------------------------------------------------------------------------
    #
    @property
    def logfile(self):
        """Returns the logfile of the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            raise IncorrectState("Invalid instance.")

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json.get ('logfile')

    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the log of the pilot.
        """
        if not self._uid:
            return None

        logs = []

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        for log in pilot_json['log']:
            logs.append(Logentry(logentry=log["logentry"], timestamp=log["timestamp"]))

        return logs

    # -------------------------------------------------------------------------
    #
    @property
    def resource_detail(self):
        """Returns the names of the nodes managed by the pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
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
            return None

        raise NotImplemented("Not Implemented")

    # -------------------------------------------------------------------------
    #
    @property
    def units(self):
        """ Returns the units scheduled for this pilot.
        """
        # Check if this instance is valid
        if not self._uid:
            return None

        raise NotImplemented("Not Implemented")

    # -------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the pilot was submitted.
        """
        # Check if this instance is valid
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['submitted']

    # -------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the pilot was started on the backend.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['started']

    # -------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the pilot was stopped.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['finished']

    # -------------------------------------------------------------------------
    #
    @property
    def resource(self):
        """ Returns the resource.
        """
        if not self._uid:
            return None

        pilot_json = self._worker.get_compute_pilot_data(pilot_ids=self.uid)
        return pilot_json['description']['resource']

    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func, callback_data=None):
        """Registers a callback function that is triggered every time the
        ComputePilot's state changes.

        All callback functions need to have the same signature::

            def callback_func(obj, state, data)

        where ``object`` is a handle to the object that triggered the callback,
        ``state`` is the new state of that object, and ``data`` is the data
        passed on callback registration.
        """
        self._worker.register_pilot_callback(self, callback_func, callback_data)

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
            raise IncorrectState("Invalid instance.")

        if not isinstance(state, list):
            state = [state]

        start_wait = time.time()
        # the self.state property pulls the state from the back end.
        new_state = self.state

        while new_state not in state:
            time.sleep(0.1)
            new_state = self.state

            if (timeout is not None) and (timeout <= (time.time() - start_wait)):
                break

        # done waiting -- return the state
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
            raise IncorrectState(msg="Invalid instance.")

        if self.state in [DONE, FAILED, CANCELED]:
            # nothing to do as we are already in a terminal state
            return

        # now we can send a 'cancel' command to the pilot.
        self._manager.cancel_pilots(self.uid)

    # -------------------------------------------------------------------------
    #
    def stage_in(self, directives):
        """Stages the content of the staging directive into the pilot's
        staging area"""

        # Wait until we can assume the pilot directory to be created
        if self.state == NEW:
            self.wait(state=[PENDING_LAUNCH, LAUNCHING, PENDING_ACTIVE, ACTIVE])
        elif self.state in [DONE, FAILED, CANCELED]:
            raise Exception("Pilot already finished, no need to stage anymore!")

        # Iterate over all directives
        for directive in expand_staging_directive(directives, logger):

            source = directive['source']
            action = directive['action']

            # TODO: verify target?
            # Convert the target_url into a SAGA Url object
            target_url = saga.Url(directive['target'])

            # Handle special 'staging' scheme
            if target_url.scheme == 'staging':
                logger.info('Operating from staging')

                # Remove the leading slash to get a relative path from the staging area
                target = target_url.path.split('/',1)[1]

                remote_dir_url = saga.Url(os.path.join(self.sandbox, STAGING_AREA))
            else:
                remote_dir_url = target_url
                remote_dir_url.path = os.path.dirname(directive['target'])
                target = os.path.basename(directive['target'])

            # Define and open the staging directory for the pilot
            remote_dir = saga.filesystem.Directory(remote_dir_url,
                                               flags=saga.filesystem.CREATE_PARENTS)

            if action == LINK:
                # TODO: Does this make sense?
                #log_message = 'Linking %s to %s' % (source, abs_target)
                #os.symlink(source, abs_target)
                pass
            elif action == COPY:
                # TODO: Does this make sense?
                #log_message = 'Copying %s to %s' % (source, abs_target)
                #shutil.copyfile(source, abs_target)
                pass
            elif action == MOVE:
                # TODO: Does this make sense?
                #log_message = 'Moving %s to %s' % (source, abs_target)
                #shutil.move(source, abs_target)
                pass
            elif action == TRANSFER:
                log_message = 'Transferring %s to %s' % (source, remote_dir_url)
                # Transfer the local file to the remote staging area
                remote_dir.copy(source, target)
            else:
                raise Exception('Action %s not supported' % action)
