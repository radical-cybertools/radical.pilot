"""
.. module:: sagapilot.unit_manager
   :platform: Unix
   :synopsis: Implementation of the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from sagapilot.compute_unit  import ComputeUnit
from sagapilot.utils.logger      import logger

import sagapilot.types       as types
import sagapilot.states      as states
import sagapilot.exceptions  as exceptions
import sagapilot.attributes  as attributes

import time
import datetime

# ------------------------------------------------------------------------------
# Attribute keys
UID               = 'UID'
SCHEDULER         = 'Scheduler'
SCHEDULER_DETAILS = 'SchedulerDetails'

# ------------------------------------------------------------------------------
#
class UnitManager(attributes.Attributes) :
    """A UnitManager manages :class:`sinon.ComputeUnit` instances which 
    represent the **executable** workload in SAGA-Pilot. A UnitManager connects 
    the ComputeUnits with one or more :class:`Pilot` instances (which represent
    the workload **executors** in SAGA-Pilot) and a **scheduler** which 
    determines which :class:`ComputeUnit` gets executed on which :class:`Pilot`.

    Each UnitManager has a unique identifier :data:`sinon.UnitManager.uid`
    that can be used to re-connect to previoulsy created UnitManager in a
    given :class:`sinon.Session`.

    **Example**::

        s = sinon.Session(database_url=DBURL)
        
        pm = sinon.PilotManager(session=s)

        pd = sinon.ComputePilotDescription()
        pd.resource = "futuregrid.alamo"
        pd.cores = 16

        p1 = pm.submit_pilots(pd) # create first pilot with 16 cores
        p2 = pm.submit_pilots(pd) # create second pilot with 16 cores

        # Create a workload of 128 '/bin/sleep' compute units
        compute_units = []
        for unit_count in range(0, 128):
            cu = sinon.ComputeUnitDescription()
            cu.executable = "/bin/sleep"
            cu.arguments = ['60']
            compute_units.append(cu)

        # Combine the two pilots, the workload and a scheduler via 
        # a UnitManager.
        um = sinon.UnitManager(session=session, scheduler="round_robin")
        um.add_pilot(p1)
        um.submit_units(compute_units)
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, session, scheduler=None, _reconnect=False) :
        """Creates a new UnitManager and attaches it to the session. 

        **Args:**

            * session (`string`): The session instance to use.

            * scheduler (`string`): The name of the scheduler plug-in to use.

        **Raises:**
            * :class:`sinon.SinonException`
        """
        self._DB = session._dbs
        self._session = session

        # initialize attributes
        attributes.Attributes.__init__(self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # The UID attribute
        self._attributes_register(UID, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(UID, self._get_uid_priv)

        # The SCHEDULER attribute
        self._attributes_register(SCHEDULER, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(SCHEDULER, self._get_scheduler_priv)

        # The SCHEDULER_DETAILS attribute
        self._attributes_register(SCHEDULER_DETAILS, None, attributes.STRING, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_getter(SCHEDULER_DETAILS, self._get_scheduler_details_priv)

        pm = ru.PluginManager ('sinon')


        self._scheduler = pm.load('unit_scheduler', scheduler)
        self._scheduler.init(self)

        if _reconnect is False:
            # Add a new unit manager netry to the DB
            self._uid = self._DB.insert_unit_manager(
                unit_manager_data={'scheduler' : scheduler})
            logger.info("Created new UnitManager %s." % str(self))

        else:
            pass
            # re-connect. do nothing

    #---------------------------------------------------------------------------
    #
    @classmethod
    def _reconnect(cls, session, unit_manager_id):

        if unit_manager_id not in session._dbs.list_unit_manager_uids():
            raise exceptions.BadParameter ("PilotManager with id '%s' not in database." % unit_manager_id)

        um_data = session._dbs.get_unit_manager(unit_manager_id)

        obj = cls(session=session, scheduler=um_data['scheduler'], _reconnect=True)
        obj._uid = unit_manager_id
        
        logger.info("Reconnected to existing UnitManager %s." % str(obj))

        return obj

    #---------------------------------------------------------------------------
    #
    def _get_uid_priv(self):
        """Returns the unit manager id.
        """
        return self._uid

    #---------------------------------------------------------------------------
    #
    def _get_scheduler_priv(self):
        """Returns the scheduler name. 
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._scheduler

    #---------------------------------------------------------------------------
    #
    def _get_scheduler_details_priv(self):
        """Returns the scheduler details, e.g., the scheduler logs. 
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        raise Exception("Not implemented")

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns information about the pilot manager as a Python dictionary.
        """
        info_dict = {
            'type'      : 'UnitManagerManager', 
            'uid'       : self._get_uid_priv(),
            'scheduler' : str(self._get_scheduler_priv())
        }
        return info_dict

    # --------------------------------------------------------------------------
    #
    def __str__(self):
        """Returns a string representation of the pilot manager.
        """
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    def add_pilots(self, pilots):
        """Associates one or more pilots with the unit manager.

        **Arguments:**

            * **pilots** [:class:`sinon.ComputePilot` or list of 
              :class:`sinon.ComputePilot`]: The pilot objects that will be 
              added to the unit manager.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance (pilots, list):
            pilots = [pilots]

        pids = []
        for pilot in pilots:
            pids.append(pilot.uid)

        self._DB.unit_manager_add_pilots(unit_manager_id=self.uid,
                                         pilot_ids=pids)

    # --------------------------------------------------------------------------
    #
    def list_pilots(self, type=types.PILOT_ANY):
        """Lists the UIDs of the pilots currently associated with
        the unit manager.

        **Arguments:**

            * **type** [`int`]: The type of pilots to list. Possible options are:

              * :data:`sinon.types.PILOT_ANY`
              * :data:`sinon.types.PILOT_DATA`
              * :data:`sinon.types.PILOT_COMPUTE`

        **Returns:**

              * A list of :class:`sinon.ComputePilot` UIDs [`string`].

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._DB.unit_manager_list_pilots(unit_manager_uid=self.uid)

    # --------------------------------------------------------------------------
    #
    def remove_pilots(self, pilot_ids, drain=True):
        """Disassociates one or more pilots from the unit manager. 

        After a pilot has been removed from a unit manager, it won't process
        any of the unit manager's units anymore. Calling `remove_pilots` doesn't 
        stop the pilot itself.

        **Arguments:**

            * **drain** [`boolean`]: Drain determines what happens to the units 
              which are managed by the removed pilot(s). If `True`, all units 
              currently assigned to the pilot are allowed to finish execution.
              If `False` (the default), then `RUNNING` units will be canceled.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance (pilot_ids, list):
            pilot_ids = [pilot_ids]

        self._DB.unit_manager_remove_pilots(unit_manager_id=self.uid,
                                            pilot_ids=pilot_ids)

    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=types.ANY) :
        """Returns the UIDs of the :class:`sinon.ComputeUnit` managed by this 
        unit manager.

        **Returns:**

              * A list of :class:`sinon.ComputeUnit` UIDs [`string`].

        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        return self._DB.unit_manager_list_work_units(unit_manager_uid=self.uid)

    # --------------------------------------------------------------------------
    #
    def submit_units(self, unit_descriptions) :
        """Submits on or more :class:`sinon.ComputeUnit` instances to the unit
        manager. 

        **Arguments:**

            * **unit_descriptions** [:class:`sinon.ComputeUnitDescription` or list of 
              :class:`sinon.ComputeUnitDescription`]: The description of the 
              compute unit instance(s) to create.

        **Returns:**

              * A list of :class:`sinon.ComputeUnit` objects.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if not isinstance(unit_descriptions, list):
            unit_descriptions = [unit_descriptions]

        from bson.objectid import ObjectId

        if True : ## always use the scheduler for now...

            if not self._scheduler :
                raise exceptions.SinonException("Internal error - no unit scheduler")

            # the scheduler will return a dictionary of the form:
            #   { 
            #     pilot_id_1  : [ud_1, ud_2, ...], 
            #     pilot_id_2  : [ud_3, ud_4, ...], 
            #     ...
            #   }
            # The scheduler may not be able to schedule some units -- those will
            # simply not be listed for any pilot.  The UM needs to make sure
            # that no UD from the original list is left untreated, eventually.
            try :
                schedule = self._scheduler.schedule (unit_descriptions)
            except Exception as e :
                raise exceptions.SinonException("Internal error - unit scheduler failed: %s" % e)

            units       = list()  # compute unit instances to return
            unscheduled = list()  # unscheduled unit descriptions

            # we copy all unit descriptions into unscheduled, and then remove
            # the scheduled ones...
            unscheduled = unit_descriptions[:]  # python semi-deep-copy magic

            # submit to all pilots which got something submitted to
            for pilot_id in schedule.keys () :

                # sanity check on scheduler provided information
                if not pilot_id in self.list_pilots () :
                    raise exceptions.SinonException("Internal error - invalid scheduler reply, "
                                        "no such pilot %s" % pilot_id)

                # get the scheduled unit descriptions for this pilot
                uds = schedule[pilot_id]

                # submit each unit description scheduled here, all in one bulk
                submission_dict = {}
                for ud in uds :

                    # sanity check on scheduler provided information
                    if  not ud in unscheduled :
                        raise exceptions.SinonException("Internal error - invalid scheduler reply, "
                                            "no such unit description %s" % ud)

                    # looks ok -- add the unit as submission candidate
                    unit_id = ObjectId()
                    submission_dict[unit_id] = {
                        'description': ud, 
                        'info': {'state': states.PENDING, 
                                 'submitted': datetime.datetime.utcnow(),
                                 'log': []}
                    }

                    # this unit is not unscheduled anumore...
                    unscheduled.remove (ud)

                # done iterating over all units, for this plot -- submit bulk 
                # for this pilot
                self._DB.insert_workunits(
                    pilot_id=pilot_id, 
                    unit_manager_uid=self.uid,
                    units=submission_dict
                )

                for unit_id, unit_desc in submission_dict.iteritems():
                    compute_unit = ComputeUnit._create(
                        unit_id=str(unit_id),
                        unit_description=unit_desc['description'], 
                        unit_manager_obj=self
                    )
                    units.append(compute_unit)

            # the schedule provided by the scheduler is now evaluated -- check
            # that we didn't lose/gain any units
            if  len(units) + len(unscheduled) != len(unit_descriptions) :
                raise exceptions.SinonException("Internal error - wrong #units returned from scheduler")

            # keep unscheduled units around for later, out-of-band scheduling
            self._unscheduled_units = unscheduled

            if len(units) == 1: 
                return units[0]
            else: 
                return units

    # --------------------------------------------------------------------------
    #
    def get_units(self, unit_ids=None):
        """Returns one or more compute units identified by their IDs. 

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the 
              compute unit objects to return.

        **Returns:**

              * A list of :class:`sinon.ComputeUnit` objects.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        pilots = ComputeUnit._get(unit_ids=unit_ids, unit_manager_obj=self)
        return pilots

    # --------------------------------------------------------------------------
    #
    def wait_units(self, unit_ids=None, state=[states.DONE, states.FAILED, states.CANCELED], timeout=None):
        """Returns when one or more :class:`sinon.ComputeUnits` reach a 
        specific state. 

        If `unit_uids` is `None`, `wait_units` returns when **all** ComputeUnits
        reach the state defined in `state`.

        **Example**::

            # TODO

        **Arguments:**

            * **unit_uids** [`string` or `list of strings`] 
              If unit_uids is set, only the ComputeUnits with the specified uids 
              are considered. If unit_uids is `None` (default), all ComputeUnits
              are considered.

            * **state** [`string`]
              The state that ComputeUnits have to reach in order for the call
              to return. 

              By default `wait_units` waits for the ComputeUnits to 
              reach a terminal state, which can be one of the following:

              * :data:`sinon.DONE`
              * :data:`sinon.FAILED`
              * :data:`sinon.CANCELED`

            * **timeout** [`float`]
              Timeout in seconds before the call returns regardless of Pilot
              state changes. The default value **None** waits forever.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        start_wait = time.time ()
        all_done   = False

        while all_done is False:

            all_done = True

            for wu_state in self._DB.get_workunit_states(workunit_manager_id=self._uid):
                if wu_state not in state:
                    all_done = False
                    break # leave for loop

            # check timeout
            if (None != timeout) and (timeout <= (time.time () - start_wait)) :
                break

            # wait a bit
            time.sleep(1)

        # done waiting
        return

    # --------------------------------------------------------------------------
    #
    def cancel_units (self, unit_ids=None):
        """Cancel one or more :class:`sinon.ComputeUnits`.

        **Arguments:**

            * **unit_ids** [`string` or `list of strings`]: The IDs of the 
              compute unit objects to cancel.

        **Raises:**

            * :class:`sinon.SinonException`
        """
        if not self._uid:
            raise exceptions.IncorrectState(msg="Invalid object instance.")

        if (not isinstance(unit_ids, list)) and (unit_ids is not None):
            unit_ids = [unit_ids]

        raise Exception("Not implemented")

