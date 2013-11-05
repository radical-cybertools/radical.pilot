"""
.. module:: sinon.unit_manager
   :platform: Unix
   :synopsis: Implementation of the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, The RADICAL Group at Rutgers University"
__license__   = "MIT"

import sinon._api as interface

from session      import Session
from pilot        import Pilot

from sinon.db import Session as dbSession

# ------------------------------------------------------------------------------
#
class UnitManager (object) :
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
        um = sinon.UnitManager(session=session, scheduler="ROUNDROBIN")
        um.add_pilot(p1)
        um.submit_units(compute_units)
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, session, unit_manager_uid=None, scheduler=None, ) :
        """Creates a new or reconnects to an exising UnitManager.

        If called without a unit_manager_uid, a new UnitManager object is 
        created and attached to the session. If unit_manager_uid is set, an 
        existing UnitManager instance is retrieved from the session. 

        **Args:**

            * session (str): The session instance to use.

            * unit_manager_uid (str): If pilot_manager_uid is set, we try 
              re-connect to an existing PilotManager instead of creating a 
              new one.

            * scheduler (str): The name of the scheduler plug-in to use.

        **Raises:**
            * :class:`sinon.SinonException`
        """
        self._DB = session._dbs
        self._session = session

        if unit_manager_uid is None:
            # Create a new unit manager.
            self._umid = self._DB.insert_unit_manager(unit_manager_data={})
        else:
            # reconnect to an existing PM
            if unit_manager_uid not in self._DB.list_unit_manager_uids():
                raise LookupError ("Unit Manager '%s' not in database." \
                    % unit_manager_uid)
            self._umid = unit_manager_uid

    #---------------------------------------------------------------------------
    #
    @property
    def umid(self):
        """ Returns the unit manager id.
        """
        return self._umid

    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pilot):
        self._DB.unit_manager_add_pilot(unit_manager_uid=self.umid,
                                        pilot_id=pilot.uid)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :
        return self._DB.unit_manager_list_pilots(unit_manager_uid=self.umid)


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=interface.ANY) :
        return self._DB.unit_manager_list_work_units(unit_manager_uid=self.umid)


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pilot_id, drain=True):
        self._DB.unit_manager_remove_pilot(unit_manager_uid=self.umid,
                                           pilot_id=pilot_id)


    # --------------------------------------------------------------------------
    #
    def submit_units(self, unit_descriptions) :
        """Docstring!
        """
        pilot_id = self.list_pilots()[0]
        self._DB.insert_workunits(pilot_id=pilot_id, 
            unit_manager_uid=self.umid,
            unit_descriptions=unit_descriptions)

        return None

        # with self._rlock :

        # # FIXME: bulk

        #     if  not descr.attribute_exists ('dtype') :
        #         raise e.BadParameter ("Invalid description (no type)")

        #     if  not descr.dtype in [ sa.COMPUTE, sa.DATA ] :
        #         raise e.BadParameter ("Unknown description type %s" % descr.dtype)

        #     if  not descr.dtype in [ sa.COMPUTE ] :
        #         raise e.BadParameter ("Only compute units are supported")

        #     unit = cu.ComputeUnit._register (descr, manager=self)
        #     pid  = None

        #     pid = None

        #     # try to schedule the unit on a pilot
        #     if  len (self.pilots)  == 0 :
        #         # nothing to schedule on...
        #         pid = None

        #     elif len (self.pilots) == 1 :
        #         # if we have only one pilot, there is not much to 
        #         # scheduler (i.e., direct submission)
        #         pid = self.pilots[0]

        #     elif not self._scheduler :
        #         # if we don't have a scheduler, we do random assignments
        #         # FIXME: we might allow user hints, you know, for 'research'?
        #         pid = random.choice (self.pilots)

        #     else :
        #         # hurray, we can use the scheduler!
        #         pid = self._scheduler.schedule (descr)

            
        #     # have a target pilot?  If so, schedule -- if not, keep around
        #     if  None == pid :
        #         # no eligible pilot, yet
        #         self._unscheduled.append (unit)

        #     else :

        #         if  not pid in self._pilots :
        #             raise e.NoSuccess ("Internal error - invalid scheduler reply")

        #         unit._submit (self._pilots[pid])


        #     return unit





    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids) :

        with self._rlock :

            # FIXME
            pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids=None, state=[interface.DONE, interface.FAILED, interface.CANCELED], timeout=-1.0) :

        with self._rlock :

            if  not isinstance (state, list) :
                state = [state]

            # FIXME
            pass


    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids) :

        with self._rlock :

            # FIXME
            pass


# ------------------------------------------------------------------------------
#


