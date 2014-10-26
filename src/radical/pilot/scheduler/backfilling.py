#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.BackfillingScheduler
   :platform: Unix
   :synopsis: A multi-pilot, backfilling scheduler.

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
class BackfillingScheduler(Scheduler):
    """
    
    BackfillingScheduler implements a multi-pilot, backfilling scheduling
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
        logger.info("Loaded scheduler: %s." % self.name)

        self.manager = manager
        self.session = session
        self.waitq   = list()
        self.runq    = list()
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

        pilot_docs = self.manager._worker._db.get_pilots (pilot_ids=self.pilots.keys ())

        for pilot_doc in pilot_docs :

            pid = str (pilot_doc['_id'])
            if  not pid in self.pilots :
                raise RuntimeError ("Got invalid pilot doc (%s)" % pid)

            self.pilots[pid]['state'] = str(pilot_doc.get ('state'))
            self.pilots[pid]['cap']   = int(pilot_doc.get ('capability', 0))


    # -------------------------------------------------------------------------
    #
    def _unit_state_callback (self, unit, state) :
        
        try :

            uid = unit.uid

            if  not unit in self.runq :
                # as we cannot unregister callbacks, we simply ignore this
                # invokation.  Its probably from a unit we handled previously.
                # (although this should have been final?)
                # FIXME: how can I *un*register a unit callback?
                return

            logger.debug ("[SchedulerCallback]: Computeunit %s changed to %s" % (uid, state))

            if  state in [PENDING_OUTPUT_STAGING, STAGING_OUTPUT, DONE, FAILED, CANCELED] :
                # the pilot which owned this CU should now have free slots available
                # FIXME: how do I get the pilot from the CU?
                
                pid = unit.execution_details.get ('pilot', None)

                if  not pid :
                    logger.warning ('cannot handle final unit %s w/o pilot information' % uid)

                if  pid not in self.pilots :
                    logger.warning ('cannot handle unit %s cb for pilot %s (pilot is gone)' % (uid, pid))

                elif unit in self.runq :
                    # only interpret this event once, on any of the states above,
                    # whichever occurs first
                    self.pilots[pid]['caps'] += unit.description.cores
                    self.runq.remove (unit)
                    self._reschedule (pid=pid)
              #     logger.debug ('unit %s frees %s cores on (-> %s)' \
              #                % (uid, unit.description.cores, pid, self.pilots[pid]['caps']))

              # else :
              #     logger.debug ('unit %s freed %s cores on %s (== %s) -- not reused!'
              #                % (uid, unit.description.cores, pid, self.pilots[pid]['caps']))

        except Exception as e :
            logger.error ("error in unit callback for backfiller (%s) - ignored" % e)


    # -------------------------------------------------------------------------
    #
    def _pilot_state_callback (self, pilot, state) :
        
        try :

            pid = pilot.uid
    
            if  not pid in self.pilots :
                # as we cannot unregister callbacks, we simply ignore this
                # invokation.  Its probably from a pilot we used previously.
                logger.warn ("[SchedulerCallback]: ComputePilot %s changed to %s (ignored)" % (pid, state))
                return
    
    
            self.pilots[pid]['state'] = state
            logger.debug ("[SchedulerCallback]: ComputePilot %s changed to %s" % (pid, state))
    
            if  state in [ACTIVE] :
                # the pilot is now ready to be used
                self._reschedule (pid=pid)
    
            if  state in [DONE, FAILED, CANCELED] :
                # we can't use this pilot anymore...  
                del self.pilots[pid]
    
                # FIXME: how can I *un*register a pilot callback?
    
        except Exception as e :
          # import traceback
          # traceback.print_exc ()
            logger.error ("error in pilot callback for backfiller (%s) - ignored" % e)


    # -------------------------------------------------------------------------
    #
    def pilot_added (self, pilot) :

        pid = pilot.uid

        # get initial information about the pilot capabilities
        #
        # NOTE: this assumes that the pilot manages no units, yet.  This will
        # generally be true, as the UM will call this methods before it submits
        # any units.  This will, however, work badly with pilots which are added
        # to more than one UM.  This though holds true for other parts in this
        # code as well, thus we silently ignore this issue for now, and accept
        # this as known limitation....
        self.pilots[pid] = dict()
        self.pilots[pid]['cores']    = pilot.description.cores
        self.pilots[pid]['caps']     = pilot.description.cores
        self.pilots[pid]['state']    = pilot.state
        self.pilots[pid]['resource'] = pilot.resource
        self.pilots[pid]['sandbox']  = pilot.sandbox

        # make sure we register callback only once per pmgr
        pmgr = pilot.pilot_manager
        if  pmgr not in self.pmgrs :
            self.pmgrs.append (pmgr)
            pmgr.register_callback (self._pilot_state_callback)

        # if we have any pending units, we better serve them now...
        self._reschedule (pid=pid)


    # -------------------------------------------------------------------------
    #
    def pilot_removed (self, pid) :

        if  not pid in self.pilots :
            raise RuntimeError ('cannot remove unknown pilot (%s)' % pid)

        del self.pilots[pid]
        # FIXME: how can I *un*register a pilot callback?

        # no need to schedule, really


    # -------------------------------------------------------------------------
    #
    def schedule (self, units) :

        # this call really just adds the incoming units to the wait queue and
        # then calls reschedule() to have them picked up.
        for unit in units :
            
            if  unit in self.waitq :
                raise RuntimeError ('Unit cannot be scheduled twice (%s)' % unit.uid)

            if  unit in self.runq :
                raise RuntimeError ('Unit cannot be scheduled twice (%s)' % unit.uid)

            if  unit.state != NEW :
                raise RuntimeError ('Unit %s not in NEW state (%s)' % unit.uid)

            self.waitq.append (unit)

        # lets see what we can do about the known units...
        self._reschedule ()


    
    # -------------------------------------------------------------------------
    #
    def unschedule (self, units) :

        # the UM revokes the control over this unit from us...

        for unit in units :

            uid = unit.uid

            if  unit in self.runq  :
                raise RuntimeError ('cannot unschedule assigned unit (%s)' % uid)

            if  not unit in self.waitq :
                raise RuntimeError ('cannot remove unknown unit (%s)' % uid)

            # NOTE: we don't care if that pilot had any CUs active -- its up to the
            # UM what happens to those.

            self.waitq.remove (unit)
            # FIXME: how can I *un*register a pilot callback?
            # FIXME: is this is a race condition with the unit state callback
            #        actions on the queues?


    # -------------------------------------------------------------------------
    #
    def _reschedule (self, pid=None) :

        # dig through the list of waiting CUs, and try to find a pilot for each
        # of them.  This enacts first-come-first-served, but will be unbalanced
        # if the units in the queue are of different sizes (it is kind of
        # opposite to backfilling -- its frontfilling ;).  That problem is
        # ignored at this point.
        #
        # if any units get scheduled, we push a dictionary to the UM to enact
        # the schedule:
        #   { 
        #     unit_1: [pilot_id_1, pilot_resource_name]
        #     unit_2: [pilot_id_2, pilot_resource_name]
        #     unit_4: [pilot_id_2, pilot_resource_name]
        #     ...
        #   }

        if  not len(self.pilots.keys ()) :
            # no pilots to  work on, yet.
            logger.warning ("cannot schedule -- no pilots available")
            return 

        if  pid and pid not in self.pilots :
            logger.warning ("cannot schedule -- invalid target pilot %s" % pid)
            raise RuntimeError ("Invalid pilot (%s)" % pid)
            

        schedule           = dict()
        schedule['units']  = dict()
        schedule['pilots'] = self.pilots

        logger.debug ("schedule (%s units waiting)" % len(self.waitq))

        # iterate on copy of waitq, as we manipulate the list during iteration.
        for unit in self.waitq[:] :

            uid = unit.uid
            ud  = unit.description

          # logger.debug ("examine unit  %s (%s cores)" % (uid, ud.cores))

            for pid in self.pilots :

              # logger.debug ("        pilot %s (%s caps, state %s)" \
              #            % (pid, self.pilots[pid]['state'], self.pilots[pid]['caps']))

                if  self.pilots[pid]['state'] in [ACTIVE] :

                    if  ud.cores <= self.pilots[pid]['caps'] :
                
                      # logger.debug ("        unit  %s fits on pilot %s" % (uid, pid))

                        # sanity check on unit state
                        if  unit.state not in [NEW] :
                            raise RuntimeError ("scheduler queue should only contain NEW units (%s)" % uid)

                        self.pilots[pid]['caps'] -= ud.cores
                        schedule['units'][unit]   = pid

                        # scheduled units are removed from the waitq
                        self.waitq.remove (unit)
                        self.runq.append  (unit)
                        break


                # unit was not scheduled...
                schedule['units'][unit] = None

            # print a warning if a unit cannot possibly be scheduled, ever
            can_handle_unit = False
            for pid in self.pilots :
                if  unit.description.cores <= self.pilots[pid]['cores'] :
                    can_handle_unit=True
                    break

            if  not can_handle_unit :
                logger.warning ('cannot handle unit %s with current set of pilots' % uid)

      # pprint.pprint (schedule)

        # tell the UM about the schedule
        self.manager.handle_schedule (schedule)

    # --------------------------------------------------------------------------

