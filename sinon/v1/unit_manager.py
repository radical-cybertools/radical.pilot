

import saga
import random

import radical.utils   as ru

import sinon.api       as sa
import sinon
from   attributes import *
from   constants  import *


# ------------------------------------------------------------------------------
#
class UnitManager (Attributes, sa.UnitManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, umid=None, scheduler=None, session=None) :

        # initialize session
        self._sid, self._root = sinon.initialize ()

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
        Attributes.__init__ (self)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # deep inspection
        self._attributes_register  ('umid',    umid,      STRING, SCALAR, READONLY)
        self._attributes_register  (SCHEDULER, scheduler, STRING, SCALAR, READONLY)
        self._attributes_register  (PILOTS,    [],        STRING, VECTOR, READONLY)
        self._attributes_register  (UNITS,     [],        STRING, VECTOR, READONLY)
        # ...

        # register state
        self._base = self._root.open_dir (self.umid, flags=saga.advert.CREATE_PARENTS)
        self._base.set_attribute ('pilots', [])
        self._base.set_attribute ('units',  [])


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pilot, async=False) :

        print pilot.pid
        print self.umid
        self.pilots.append (pilot.pid)
        self._base = self._root.open_dir (self.umid + '/' + pilot.pid, flags=saga.advert.CREATE_PARENTS)
        pass


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=True, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, descr, async=False) :

        # FIXME: async, bulk

        if  not descr.attribute_exists ('dtype') :
            raise sinon.BadParameter ("Invalid description (no type)")

        if  not descr.dtype in [ sinon.COMPUTE, sinon.DATA ] :
            raise sinon.BadParameter ("Unknown description type %s" % descr.dtype)

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


        if  descr.dtype == sinon.COMPUTE :
            unit = sinon.ComputeUnit._create (descr, self, pid)
        else :
            unit = sinon.DataUnit._create (descr, self, pid)


        return unit


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=ANY, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def get_unit (self, uids, async=False) :

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def wait_units (self, uids, state=[DONE, FAILED, CANCELED], timeout=-1.0, async=False) :

        if  not isinstance (state, list) :
            state = [state]

        # FIXME
        pass


    # --------------------------------------------------------------------------
    #
    def cancel_units (self, uids, async=False) :

        # FIXME
        pass


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

