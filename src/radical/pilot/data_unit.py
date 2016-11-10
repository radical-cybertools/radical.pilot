#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.compute_unit
   :platform: Unix
   :synopsis: Implementation of the DataUnit class.
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import copy
import time

import radical.utils as ru


from .states      import *
from .logentry    import *
from .exceptions  import *
from .utils import logger
from .db.database import COMMAND_CANCEL_COMPUTE_UNIT
from .staging_directives import expand_staging_directive

# -----------------------------------------------------------------------------
#
class DataUnit(object):
    """
    A DataUnit represents a self-contained, related set of data.
    A DataUnit is defined as an immutable container for a logical group of
    "affine" data files, e. g. data that is often accessed together
    e.g. by multiple DataUnits.

    This simplifies distributed data management tasks, such as data placement,
    replication and/or partitioning of data, abstracting low-level details,
    such as storage service specific access detail.

    A DU is completely decoupled from its physical location and can be
    stored in different kinds of backends, e. g. on a parallel filesystem,
    cloud storage or in-memory.

    Replicas of a DU can reside in different DataPilots.

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid=None):

        # 'static' members
        self._uid         = None
        self._name        = None
        self._description = None
        self._manager     = None
        self._existing    = None

        # handle to the manager's worker
        self._worker = None


    #--------------------------------------------------------------------------
    #
    def __repr__(self):

        return "%s (%-15s: %s) (%s)" % (self.uid, self.state,
                                           self.description.name,
                                           id(self))

    # -------------------------------------------------------------------------
    #
    @staticmethod
    def create(unit_manager_obj, unit_description, local_state):
        """ PRIVATE: Create a new compute unit.
        """
        # create and return pilot object
        dataunit = DataUnit()

        # Make a copy of the UD to work on without side-effects.
        ud_copy = copy.deepcopy(unit_description)

        # sanity check on description
        if not 'files' in unit_description or not unit_description['files']:
            raise PilotException("ComputeUnitDescription needs files member")

        dataunit._description = ud_copy
        dataunit._manager = unit_manager_obj
        dataunit._session = unit_manager_obj._session
        dataunit._worker = unit_manager_obj._worker
        dataunit._uid = ru.generate_id('du.%(counter)06d',
                                          ru.ID_CUSTOM)
        dataunit._name = unit_description['name']
        dataunit._pilot_ids = []
        dataunit._local_state = local_state

        dataunit._session.prof.prof('advance', msg=NEW, uid=dataunit._uid, state=NEW)


        return dataunit


    # --------------------------------------------------------------------------
    #
    def import_data(self, src):
        """
        For a data unit which does not point to PFNs yet, create a first PFN as
        copy from the given src URL.

        FIXME: what happens if we already have PFNs?
        """
        pass


    # --------------------------------------------------------------------------
    #
    def export_data(self, tgt):
        """
        Copy any of the data_unit's PFNs to the tgt URL.
        """
        pass


    # --------------------------------------------------------------------------
    #
    def remove_data(self):
        """
        Removes the data.  Implies cancel ()
        """
        pass


    # -------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        obj_dict = {
            'uid':               self.uid,
            'name':              self.name,
            'state':             self.state,
            'log':               self.log,
            'submission_time':   self.submission_time,
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

        The uid identifies the DataUnit within a :class:`UnitManager` and
        can be used to retrieve an existing DataUnit.

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
    def sandbox(self):
        """Returns the full working directory URL of this DataUnit.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def pilot_ids(self):
        """Returns the pilot_id of this DataUnit.
        """
        return self._pilot_ids


    # -------------------------------------------------------------------------
    #
    @pilot_ids.setter
    def pilot_ids(self, value):
        self._pilot_ids = value


    # -------------------------------------------------------------------------
    #
    @property
    def description(self):
        """Returns the DataUnitDescription the DataUnit was started with.
        """
        return self._description


    # -------------------------------------------------------------------------
    #
    @property
    def state(self):
        """Returns the current state of the DataUnit.
        """
        return self._local_state


    # -------------------------------------------------------------------------
    #
    @property
    def state_history(self):
        """Returns the complete state history of the DataUnit.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def callback_history(self):
        """Returns the complete callback history of the DataUnit.
        """
        pass



    # -------------------------------------------------------------------------
    #
    @property
    def log(self):
        """Returns the logs of the DataUnit.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def submission_time(self):
        """ Returns the time the DataUnit was submitted.
        """
        pass

    # -------------------------------------------------------------------------
    #
    @property
    def start_time(self):
        """ Returns the time the DataUnit was started on the backend.
        """
        pass


    # -------------------------------------------------------------------------
    #
    @property
    def stop_time(self):
        """ Returns the time the DataUnit was stopped.
        """
        pass


    # -------------------------------------------------------------------------
    #
    def register_callback(self, callback_func, callback_data=None):
        """Registers a callback function that is triggered every time the
        DataUnit's state changes.

        All callback functions need to have the same signature::

            def callback_func(obj, state)

        where ``object`` is a handle to the object that triggered the callback
        and ``state`` is the new state of that object.
        """
        pass


    # -------------------------------------------------------------------------
    #
    def wait(self, state=[DONE, FAILED, CANCELED],
             timeout=None):
        """Returns when the DataUnit reaches a specific state or
        when an optional timeout is reached.

        **Arguments:**

            * **state** [`list of states`]
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
        pass


    # -------------------------------------------------------------------------
    #
    def cancel(self):
        """Cancel the DataUnit.

        **Raises:**

            * :class:`radical.pilot.radical.pilotException`
        """
        pass


# ------------------------------------------------------------------------------

