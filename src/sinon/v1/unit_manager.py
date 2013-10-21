

import random
import threading

import radical.utils  as ru

import session        as s
import attributes     as att
import exceptions     as e
import compute_unit   as cu
import data_unit      as du

import sinon._api     as sa

# ------------------------------------------------------------------------------
#
class UnitManager (att.Attributes, sa.UnitManager) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, umid=None, scheduler=None, session=None) :

        # initialize 
        self._rlock       = threading.RLock ()
        self._sid         = s.initialize ()

        # get a unique ID if none was given -- otherwise we reconnect
        if  not umid :
            self.umid = ru.generate_id ('um.')
        else :
            self.umid = str(umid)

        # load and initialize the given scheduler
        if  scheduler == None :
            self._scheduler = None

        else :
            self._supm      = ru.PluginManager ('sinon')
            self._scheduler = self._supm.load  (ptype='unit_scheduler', pname=scheduler)
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

        # private attributes
        self._attributes_register  ('_pilots',      {},        att.ANY,    att.VECTOR, att.WRITEABLE)
        self._attributes_register  ('_unscheduled', [],        att.ANY,    att.VECTOR, att.WRITEABLE)


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pilot) :

        with self._rlock :

            if  pilot.pid in self.pilots :
                raise e.BadParameter ("Pilot '%s' is already used" % pilot)

            self.pilots.append (pilot.pid)
            self._pilots[pilot.pid] = pilot

            # any units pending?
            if  len(self._unscheduled) :
                for unit in self._unscheduled :
                    print "scheduled to %s" % pilot.pid
                    unit._submit (pilot)
                    # FIXME: check for success

                self._unscheduled = []


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=sa.ANY) :

        with self._rlock :

            # FIXME: interpret ptype

            return list(self.pilots)


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=True) :

        with self._rlock :

            if  not pid in self.pilots :
                raise e.DoesNotExist ("unknown pilot '%s'" % pid)
            
            self.pilots.remove  (pid)
            self._pilots.remove (pid)


    # --------------------------------------------------------------------------
    #
    def submit_unit (self, descr) :

        with self._rlock :

        # FIXME: bulk

            if  not descr.attribute_exists ('dtype') :
                raise e.BadParameter ("Invalid description (no type)")

            if  not descr.dtype in [ sa.COMPUTE, sa.DATA ] :
                raise e.BadParameter ("Unknown description type %s" % descr.dtype)

                if  not descr.dtype in [ sa.COMPUTE ] :
                    raise e.BadParameter ("only compute units are supported")

                unit = cu.ComputeUnit._register (descr, manager=self)

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

                if  None == pid :
                    # no eligible pilot, yet
                    self._unscheduled.append (unit)

                else :

                    if  not pid in self._pilots :
                        raise e.NoSuccess ("Internal error - invalid scheduler reply")

                    unit._submit (self._pilots[pid])

            return unit


    # --------------------------------------------------------------------------
    #
    def list_units (self, utype=sa.ANY) :

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
    def wait_units (self, uids, state=[sa.DONE, sa.FAILED, sa.CANCELED], timeout=-1.0) :

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
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

