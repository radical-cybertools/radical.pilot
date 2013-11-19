"""
.. module:: sinon.unit_manager
   :platform: Unix
   :synopsis: Implementation of the UnitManager class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"


from sinon.constants import * 
from sinon.utils     import as_list

from sinon.db import Session as dbSession

from sinon.frontend.session      import Session
from sinon.frontend.pilot        import Pilot


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
    def __init__ (self, session, scheduler=None) :
        """Creates a new UnitManager and attaches it to the session. 

        **Args:**

            * session (`string`): The session instance to use.

            * scheduler (`string`): The name of the scheduler plug-in to use.

        **Raises:**
            * :class:`sinon.SinonException`
        """
        self._DB = session._dbs
        self._session = session

        if scheduler == "~+=RECON=+~":
            # When we get the "~RECON~" keyword as scheduler, we were called 
            # from the 'get()' class method
            pass
        else:
            self._uid = self._DB.insert_unit_manager(unit_manager_data={})

    # --------------------------------------------------------------------------
    #
    @classmethod 
    def get(cls, session, unit_manager_uid) :
        """ Re-connects to an existing UnitManager via its uid.

        **Example**::

            s = sinon.Session(database_url=DBURL)
            
            um1 = sinon.UnitManager(session=s)
            # Re-connect via the 'get()' method.
            um2 = sinon.UnitManager.get(session=s, unit_manager_uid=um1.uid)

            # pm1 and pm2 are pointing to the same UnitManager
            assert um1.uid == um2.uid

        **Arguments:**

            * **session** (:class:`sinon.Session`): The session instance to use.

            * **unit_manager_uid** (`string`): The unique identifier of the 
              UnitManager we want to re-connect to.

        **Raises:**
            * :class:`sinon.SinonException` if a UnitManager with 
              `unit_manager_uid` doesn't exist in the database.
        """
        if unit_manager_uid not in session._dbs.list_unit_manager_uids():
            raise LookupError ("UnitManager '%s' not in database." % pilot_manager_uid)

        obj = cls(session=session, scheduler="~+=RECON=+~")
        obj._uid = unit_manager_uid

        return obj

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """ Returns the unit manager id.
        """
        return self._uid

    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pilot):
        self._DB.unit_manager_add_pilot(unit_manager_uid=self.uid,
                                        pilot_id=pilot.uid)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :
        return self._DB.unit_manager_list_pilots(unit_manager_uid=self.uid)


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY) :
        return self._DB.unit_manager_list_work_units(unit_manager_uid=self.uid)


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pilot_id, drain=True):
        self._DB.unit_manager_remove_pilot(unit_manager_uid=self.uid,
                                           pilot_id=pilot_id)


    # --------------------------------------------------------------------------
    #
    def submit_units(self, unit_descriptions) :
        """Docstring!
        """

        ###################################################
        # ASHLEY:
        # 
        # CURRENTLY THIS SUBMITS TO THE FIRST PILOT ONLY. 
        # THIS IS OBVIOUSLY WRONG / SIMPLIFIED -- 
        # THE REAL SCHEDULER CODE IS COMMENTED-OUT BELOW.
        ####################################################

        pilot_id = self.list_pilots()[0]

        self._DB.insert_workunits(pilot_id=pilot_id, 
            unit_manager_uid=self.uid,
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
        pass


    # --------------------------------------------------------------------------
    #
    def wait_units(self, unit_uids=None, state=[DONE, FAILED, CANCELED], timeout=-1.0):
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
              state changes. The default value **-1.0** waits forever.

        **Raises:**

            * :class:`sinon.SinonException`
        """

        all_done = False

        while all_done is not True:
            for workunit in self._DB.get_workunits(workunit_manager_uid=self.uid):
                print workunit
                if workunit['info']['state'] in state:
                    all_done = True
                else:
                    all_done = False

    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids) :
        pass



