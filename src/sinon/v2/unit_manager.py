__copyright__ = "Copyright 2013, RADICAL Group @ Rutgers"
__license__   = "MIT"

import sinon._api as interface

from session      import Session
from pilot        import Pilot

from sinon.db import Session as dbSession

# ------------------------------------------------------------------------------
#
class UnitManager (object) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, unit_manager_id=None, scheduler=None, session=None) :
        """ Le constructeur.
        """
        self._DB = session._dbs
        self._session = session

        if unit_manager_id is None:
            # Create a new unit manager.
            self._umid = self._DB.insert_unit_manager(unit_manager_data={})
        else:
            # reconnect to an existing PM
            if unit_manager_id not in self._DB.list_unit_manager_ids():
                raise LookupError ("Unit Manager '%s' not in database." \
                    % unit_manager_id)
            self._umid = unit_manager_id

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
        self._DB.unit_manager_add_pilot(unit_manager_id=self.umid,
                                        pilot_id=pilot.id)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self) :
        return self._DB.unit_manager_list_pilots(unit_manager_id=self.umid)


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pilot_id, drain=True):
        self._DB.unit_manager_remove_pilot(unit_manager_id=self.umid,
                                           pilot_id=pilot_id)


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, descr) :
        pass
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
    def list_units (self, utype=interface.ANY) :

        with self._rlock :

            # FIXME
            pass


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids) :

        with self._rlock :

            # FIXME
            pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids, state=[interface.DONE, interface.FAILED, interface.CANCELED], timeout=-1.0) :

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


