#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.LateBindingScheduler
   :platform: Unix
   :synopsis: A multi-pilot, late-binding scheduler.

.. moduleauthor:: Mark Santcroos <mark.santcroos@rutgers.edu>
"""

__copyright__ = "Copyright 2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os 
import pprint

from radical.pilot.utils.logger        import logger
from radical.pilot.scheduler.interface import Scheduler
from radical.pilot.states              import *

# -----------------------------------------------------------------------------
# 
class LateBindingScheduler(Scheduler):
    """
    
    LateBindingScheduler implements a multi-pilot, late-binding scheduling
    algorithm. Only schedules CUs to Pilots that are active and have
    a free-slot.

    This scheduler is not able to handle pilots which serve more than one unit
    manager concurrently.

    """

    # -------------------------------------------------------------------------
    #
    def __init__ (self, manager, session):
        """
        """
        Scheduler.__init__ (self)
        logger.info("Loaded scheduler: %s." % self.name)

        self._name   = self.__class__.__name__
        self.manager = manager
        self.session = session
        self.waitq   = list()
        self.pmgrs   = list()
        self.pilots  = dict()

        # make sure the UM notifies us on all unit state changes
        manager.register_callback (self._unit_state_callback)


    # -------------------------------------------------------------------------
    #
    def _update_caps (self) :

        # we keep track of available cores on our own.  However, we should sync
        # our bookkeeping with reality now and then...  
        #
        # NOTE: Not sure yet when this methid will be called -- calling it too
        # frequently will slow scheduling down significantly, calling it very
        # infrequently can result in invalid schedules.  Accuracy vs.
        # performance...

        # provide revers pilot lookup
        pilot_ids = dict()
        for pilot in self.pilots :
            pilod_ids[pilot.uid] = pilot

        pilot_docs = self.manager._worker._db.get_pilots (pilot_ids=pilot_ids.keys ())

        for pilot_doc in pilot_docs :

            pid = str (pilot_doc['_id'])
            if  not pid in pilot_ids :
                raise RuntimeError ("Got invalid pilot doc (%s)" % pid)

            self.pilots[pilot_ids]['state'] = str(pilot_doc.get ('state'))
            self.pilots[pilot_ids]['cap']   = int(pilot_doc.get ('capability', 0))

        pprint.pprint (self.pilots)


    # -------------------------------------------------------------------------
    #
    def _unit_state_callback (self, unit, state) :
        

        if  not unit in self.waitq :
            # as we cannot unregister callbacks, we simply ignore this
            # invokation.  Its probably from a unit we handled previously.
            # (although this should have been final?)
            logger.warn ("[SchedulerCallback]: ComputeUnit %s changed to %s (ignored)" % (unit.uid, state))
            return

        logger.debug ("[SchedulerCallback]: Computeunit %s changed to %s" % (unit.uid, state))

        if  state in [DONE, FAILED, CANCELED] :
            # the pilot which owned this CU should now have free slots available
            # FIXME: how do I get the pilot from the CU?
            pilot = unit.pilot

            if  pilot not in self.pilots :
                raise RuntimeError ('cannot handle unit %s of pilot %s' % (unit.uid, pilot.uid))

            self.pilots[pilot]['caps'] += unit.description.cores
            self._reschedule (pilot=pilot)

            # FIXME: how can I *un*register a unit callback?


    # -------------------------------------------------------------------------
    #
    def _pilot_state_callback (self, pilot, state) :
        
        if  not pilot in self.pilots :
            # as we cannot unregister callbacks, we simply ignore this
            # invokation.  Its probably from a pilot we used previously.
            logger.warn ("[SchedulerCallback]: ComputePilot %s changed to %s (ignored)" % (pilot.uid, state))
            return


        self.pilots[pilot]['state'] = state
        logger.debug ("[SchedulerCallback]: ComputePilot %s changed to %s" % (pilot.uid, state))

        if  state in [ACTIVE] :
            # the pilot is now ready to be used
            self._reschedule (pilot=pilot)

        if  state in [DONE, FAILED, CANCELED] :
            # we can't use this pilot anymore...  
            self.pilots.remove (pilot)

            # FIXME: how can I *un*register a pilot callback?


    # -------------------------------------------------------------------------
    #
    def pilot_added (self, pilot) :

        if  pilot in self.pilots :
            raise RuntimeError ('cannot add pilot twice (%s)' % pilot.uid)

        # get initial information about the pilot capabilities
        #
        # NOTE: this assumes that the pilot manages no units, yet.  This will
        # generally be true, as the UM will call this methods before it submits
        # any units.  This will, however, work badly with pilots which are added
        # to more than one UM.  This though holds true for other parts in this
        # code as well, thus we silently ignore this issue for now, and accept
        # this as known limitation....
        self.pilots[pilot] = dict()
        self.pilots[pilot]['caps']  = pilot.description.cores
        self.pilots[pilot]['state'] = pilot.state

        # make sure we register callback only once per pmgr
        pmgr = pilot.pilot_manager
        if  pmgr not in self.pmgrs :
            self.pmgrs.append (pmgr)
            pmgr.register_callback (self._pilot_state_callback)

        # if we have any pending units, we better serve them now...
        self._reschedule (pilot=pilot)


    # -------------------------------------------------------------------------
    #
    def pilot_removed (self, pilot) :

        if  not pilot in self.pilots :
            raise RuntimeError ('cannot remove unknown pilot (%s)' % pilot.uid)

        # NOTE: we don't care if that pilot had any CUs active -- its up to the
        # UM what happens to those.

        self.pilots.remove (pilot)
        # FIXME: how can I *un*register a pilot callback?


    # -------------------------------------------------------------------------
    #
    def unit_remove (self, unit) :

        # the UM revokes the control over this unit from us...

        if  not unit in units :
            raise RuntimeError ('cannot remove unknown unit (%s)' % unit.uid)

        # NOTE: we don't care if that pilot had any CUs active -- its up to the
        # UM what happens to those.

        self.waitq.remove (unit)
        # FIXME: how can I *un*register a pilot callback?


    # -------------------------------------------------------------------------
    #
    def schedule (self, units) :

        # this call really just adds the incoming units to the wait queue and
        # then calls reschedule() to have them picked up.
        for unit in units :
            
            if  unit in self.waitq :
                raise RuntimeError ('Unit cannot be scheduled twice (%s)' % unit.uid)

            if  unit.state != NEW :
                raise RuntimeError ('Unit %s not in NEW state (%s)' % unit.uid)

        self.waitq += units

        self._reschedule ()


    
    # -------------------------------------------------------------------------
    #
    def _reschedule (self, pilot=None) :

        # dig through the list of waiting CUs, and try to find a pilot for each
        # of them.  This enacts first-come-first-served, but will be unbalanced
        # if the units in the queue are of different sizes (it is kind of
        # opposite to backfilling -- its frontfilling ;).  That problem is
        # ignored at this point.
        #
        # if any units get scheduled, we push a dictionary to the UM to enact
        # the schedule:
        #   { 
        #     unit_1: pilot_id_1
        #     unit_2: pilot_id_2
        #     unit_4: pilot_id_2
        #     ...
        #   }

        if  not len(self.pilots.keys ()) :
            # no pilots to  work on, yet.
            return 

        if  pilot and pilot not in self.pilots :
            raise RuntimeError ("Invalid pilot (%s)" % pilot.uid)
            

        print "Late-binding re-scheduling of %s units" % len(self.waitq)
        print "-------"
        pprint.pprint (self.pilots)
        print "-------"
        pprint.pprint (self.waitq)
        print "-------"


        schedule = dict()

        for unit in self.waitq:

            ud = unit.description

            for pilot in self.pilots :

                if  self.pilots[pilot]['state'] in [ACTIVE] :
                    print "%s is   active (%s)" % (pilot.uid, self.pilots[pilot]['caps'])

                    if  ud.cores < self.pilots[pilot]['caps'] :
                        self.pilots[pilot]['caps'] -= ud.cores
                        schedule[unit] = pilot.uid
                        break
                else :
                    print "%s is ! active" % pilot.uid

                # unit was not scheduled...
                schedule[unit] = None
                     
        pprint.pprint (schedule)
        self.manager.handle_schedule (schedule)

