#pylint: disable=C0301, C0103, W0212

"""
.. module::   radical.pilot.data_pilot
   :platform: Unix
   :synopsis: Provides the DataPilot class.
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import os
import time
import saga

from .states       import *
from .logentry     import *
from .exceptions   import *
from .utils        import logger

from radical.pilot.staging_directives import TRANSFER, COPY, LINK, MOVE, \
    STAGING_AREA, expand_staging_directive

# -----------------------------------------------------------------------------
#
class DataPilot (object):
    """A DataPilot represent a resource overlay on a local or remote
       resource.

    .. note:: A DataPilot cannot be created directly. The factory method
              :meth:`radical.pilot.PilotManager.submit_pilots` has to be used instead.

                **Example**::

                      pm = radical.pilot.PilotManager(session=s)

                      pd = radical.pilot.DataPilotDescription()
                      pd.resource = "local.localhost"
                      pd.size     = 60 # Megabytes
                      pd.runtime  =  5 # minutes

                      pilot = pm.submit_pilots(pd)
    """
    # -------------------------------------------------------------------------
    #
    def __init__(self, uid=None):
        pass


    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the
           DataPilot object.
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
        """Returns a string representation of the DataPilot object.
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
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the pilot.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def callback_history(self):
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def stdout(self):
        pass

    # -------------------------------------------------------------------------
    #
    @property
    def stderr(self):
        pass

    # -------------------------------------------------------------------------
    #
    @property
    def logfile(self):
        pass

    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def resource_detail(self):
        """Returns the names of the nodes managed by the pilot.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def pilot_manager(self):
        """ Returns the pilot manager object for this pilot.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def unit_managers(self):
        """ Returns the unit manager object UIDs for this pilot.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def units(self):
        """ Returns the units scheduled for this pilot.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def resource(self):
        return self.description.resource


    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func, callback_data=None):
        pass


    # -------------------------------------------------------------------------
    #
    def wait(self, state=None, timeout=None):
        pass


    # -------------------------------------------------------------------------
    #
    def cancel(self):
        pass


    # -------------------------------------------------------------------------
    #
    def stage_in(self, directives):
        """
        Stages the content of the staging directive into the pilot's
        staging area
        """
        pass


    # -------------------------------------------------------------------------
    #
    def stage_out(self, directives):
        """
        Stages the content of the staging directive from the pilot's
        staging area
        """
        pass

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def create(pilot_manager_obj, pilot_description):
        """ PRIVATE: Create a new pilot.
        """
        # Create and return pilot object.
        pilot = DataPilot()

        #pilot._uid = pilot_uid
        pilot._description = pilot_description
        pilot._manager = pilot_manager_obj

        # Pilots use the worker of their parent manager.
        pilot._worker = pilot._manager._worker

        #logger.info("Created new DataPilot %s" % str(pilot))
        return pilot


# ------------------------------------------------------------------------------

