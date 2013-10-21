

import saga
import random

import radical.utils   as ru

import sinon._api      as sa
import attributes      as att
import compute_unit    as cu
import data_unit       as du
import exceptions      as e


# ------------------------------------------------------------------------------
#
class UnitManager (att.Attributes, sa.UnitManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, umid=None, scheduler=None, session=None) :

        # initialize session
        self._sid = sinon.initialize ()

        # get a unique ID if none was given -- otherwise we reconnect
        if  not umid :
            self.umid = ru.generate_id ('um.')
        else :
            self.umid = str(umid)

        # load and initialize the given scheduler
        if  scheduler == None :
            self._scheduler = None

        else :
            self._supm      = ru.PluginManager ('sinon', 'unit_scheduler')
            self._scheduler = self._supm.load  (name=scheduler)
            self._scheduler.init (manager=self)

        # initialize attributes
        att.Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  ('umid',       umid,      att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.SCHEDULER, scheduler, att.STRING, att.SCALAR, att.READONLY)
        self._attributes_register  (sa.PILOTS,    [],        att.STRING, att.VECTOR, att.READONLY)
        self._attributes_register  (sa.UNITS,     [],        att.STRING, att.VECTOR, att.READONLY)
        # ...


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pilot) :

        print pilot.pid
        print self.umid
        self.pilots.append (pilot.pid)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=True) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, descr) :

        # FIXME: bulk

        if  not descr.attribute_exists ('dtype') :
            raise e.BadParameter ("Invalid description (no type)")

        if  not descr.dtype in [ sa.COMPUTE, sa.DATA ] :
            raise e.BadParameter ("Unknown description type %s" % descr.dtype)

        pid = None

        # try to schedule the unit on a pilot
        if  len (self.pilots)  == 0 :
            # nothing to schedule on...
            pid = None

        elif len (self.pilots) == 1 :
            # if we have only one pilot, there is not much to 
            # scheduler (i.e., direct submission)
            pid = self.pilots[0]

        elif not self._scheduler :
            # if we don't have a scheduler, we do random assignments
            # FIXME: we might allow user hints, you know, for 'research'?
            pid = random.choice (self.pilots)

        else :
            # hurray, we can use the scheduler!
            pid = self._scheduler.schedule (descr)


        if  descr.dtype == sa.COMPUTE :
            unit = cu.ComputeUnit._create (descr, self, pid)
        else :
            unit = du.DataUnit._create (descr, self, pid)


        return unit


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids, state=[DONE, FAILED, CANCELED], timeout=-1.0) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

