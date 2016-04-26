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
import threading

from ..states   import *
from ..utils    import logger
from ..utils    import timestamp

from .interface import Scheduler

# to reduce roundtrips, we can oversubscribe a pilot, and schedule more units
# than it can immediately execute.  Value is in %.
OVERSUBSCRIPTION_RATE = 0

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
        self.waitq   = dict()
        self.runqs   = dict()
        self.pmgrs   = list()
        self.pilots  = dict()
        self.lock    = threading.RLock ()
        self._dbs    = self.session.get_dbs()

        # make sure the UM notifies us on all unit state changes
        manager.register_callback (self._unit_state_callback)


    # -------------------------------------------------------------------------
    #
    def _dump (self, msg=None) :

        import pprint 
        print '----------------------------------------'
        if msg:
            print msg
        print 'session'
        print self.session.uid
        print 'waitq'
        pprint.pprint (self.waitq)
        for pid in self.runqs :
            print 'runq [%s]' % pid
            pprint.pprint (self.runqs[pid])
        print 'pilots'
        for pid in self.pilots :
            print "%s (%-15s: %s)" % (pid, self.pilots[pid]['state'], self.pilots[pid]['resource'])
        print '----------------------------------------'


    # -------------------------------------------------------------------------
    #
    def _unit_state_callback (self, unit, state) :
        
        try :

            with self.lock :
            
                uid = unit.uid

                logger.info ("[SchedulerCallback]: Computeunit %s changed to %s" % (uid, state))


                found_unit = False
                if  state in [NEW, UNSCHEDULED] :

                    for pid in self.runqs :

                        if  not pid :
                            logger.warning ('cannot handle final unit %s w/o pilot information' % uid)

                        if  uid in self.runqs[pid] :

                            logger.info ('reschedule NEW unit %s from %s' % (uid, pid))

                            unit       = self.runqs[pid][uid]
                            found_unit = True

                            del self.runqs[pid][uid]
                            self.waitq[uid] = unit

                          # self._dump ('before reschedule %s' % uid)
                            self._reschedule (uid=uid)
                          # self._dump ('after  reschedule %s' % uid)

                            return

              # if  not found_unit and uid not in self.waitq :
              #     # as we cannot unregister callbacks, we simply ignore this
              #     # invokation.  Its probably from a unit we handled previously.
              #     # (although this should have been final?)
              #     #
              #     # FIXME: how can I *un*register a unit callback?
              #     logger.error ("[SchedulerCallback]: cannot handle unit %s" % uid)
              #     self._dump()
              #     return

                if  state in [PENDING_OUTPUT_STAGING, STAGING_OUTPUT, DONE, FAILED, CANCELED] :
                    # the pilot which owned this CU should now have free slots available
                    # FIXME: how do I get the pilot from the CU?
                    
                    pid = unit.execution_details.get ('pilot', None)

                    if  not pid :
                        raise RuntimeError ('cannot handle final unit %s w/o pilot information' % uid)

                    if  pid not in self.pilots :
                        logger.warning ('cannot handle unit %s cb for pilot %s (pilot is gone)' % (uid, pid))

                    else :
                        if  uid in self.runqs[pid] :

                            unit = self.runqs[pid][uid]

                            del self.runqs[pid][uid]
                            self.pilots[pid]['caps'] += unit.description.cores
                            self._reschedule (target_pid=pid)
                            found_unit = True

                      #     logger.debug ('unit %s frees %s cores on (-> %s)' \
                      #                % (uid, unit.description.cores, pid, self.pilots[pid]['caps']))

                  # FIXME: this warning should not come up as frequently as it
                  #        does -- needs investigation!
                  # if not found_unit :
                  #     # TODO: pid can not be in self.pilots[]
                  #     logger.warn ('unit %s freed %s cores on %s (== %s) -- not reused'
                  #               % (uid, unit.description.cores, pid, self.pilots[pid]['caps']))

        except Exception as e :
            logger.exception ("error in unit callback for backfiller (%s) - ignored" % e)


    # -------------------------------------------------------------------------
    #
    def _pilot_state_callback (self, pilot, state) :
        
        try :

            with self.lock :

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
                    self._reschedule (target_pid=pid)
    
                if  state in [DONE, FAILED, CANCELED] :

                  # self._dump ('pilot is final')

                    # If the pilot state is 'DONE', 'FAILED' or 'CANCELED', we
                    # need to reschedule the units which are reschedulable --
                    # all others are marked 'FAILED' if they are already
                    # 'EXECUTING' and not restartable
                    ts = timestamp()
                    self._dbs.change_compute_units (
                        filter_dict = {"pilot"       : pid, 
                                       "state"       : {"$in": [UNSCHEDULED,
                                                                SCHEDULING,
                                                                PENDING_INPUT_STAGING,
                                                                STAGING_INPUT,
                                                                AGENT_STAGING_INPUT_PENDING,
                                                                AGENT_STAGING_INPUT,
                                                                ALLOCATING_PENDING,
                                                                ALLOCATING,
                                                                EXECUTING_PENDING,
                                                                EXECUTING,
                                                                AGENT_STAGING_OUTPUT_PENDING,
                                                                AGENT_STAGING_OUTPUT,
                                                                PENDING_OUTPUT_STAGING,
                                                                STAGING_OUTPUT]}},
                        set_dict    = {"state"       : UNSCHEDULED, 
                                       "pilot"       : None},
                        push_dict   = {"statehistory": {"state"     : UNSCHEDULED, 
                                                        "timestamp" : ts}, 
                                       "log"         : {"message"   :  "reschedule unit", 
                                                        "timestamp" : ts}
                                      })

                    self._dbs.change_compute_units (
                        filter_dict = {"pilot"       : pid, 
                                       "restartable" : True, 
                                       "state"       : {"$in": [EXECUTING, 
                                                                AGENT_STAGING_OUTPUT_PENDING,
                                                                AGENT_STAGING_OUTPUT,
                                                                PENDING_OUTPUT_STAGING,
                                                                STAGING_OUTPUT]}},
                        set_dict    = {"state"       : UNSCHEDULED,
                                       "pilot"       : None},
                        push_dict   = {"statehistory": {"state"     : UNSCHEDULED,
                                                        "timestamp" : ts}, 
                                       "log"         : {"message"   :  "reschedule unit", 
                                                        "timestamp" : ts}
                                      })

                    self._dbs.change_compute_units (
                        filter_dict = {"pilot"       : pid, 
                                       "restartable" : False, 
                                       "state"       : {"$in": [EXECUTING, 
                                                                AGENT_STAGING_OUTPUT_PENDING,
                                                                AGENT_STAGING_OUTPUT,
                                                                PENDING_OUTPUT_STAGING, 
                                                                STAGING_OUTPUT]}},
                        set_dict    = {"state"       : FAILED},
                        push_dict   = {"statehistory": {"state"     : FAILED, 
                                                        "timestamp" : ts}, 
                                       "log"         : {"message"   :  "reschedule unit", 
                                                        "timestamp" : ts}
                                      })

                        # make sure that restartable units got back into the
                        # wait queue
                        #
                        # FIXME AM: fucking state management: I don't have the
                        # unit state!  New state was just pushed to the DB, but
                        # I have actually no idea for which units, and the state
                        # known to the worker (i.e. the cached state) is most
                        # likely outdated. 
                        #
                        # So we don't handle runq/waitq here.  Instead, we rely
                        # on the unit cb to get invoked as soon as the state
                        # propagated back to us, and then remove them from the
                        # runq.  This is slow, potentially very slow, but save.
                        

                    # we can't use this pilot anymore...  
                    del self.pilots[pid]
                    # FIXME: how can I *un*register a pilot callback?
                    
    
        except Exception as e :
          # import traceback
          # traceback.print_exc ()
            logger.exception ("error in pilot callback for backfiller (%s) - ignored" % e)
            raise


    # -------------------------------------------------------------------------
    #
    def pilot_added (self, pilot) :

        with self.lock :

            pid = pilot.uid

            # get initial information about the pilot capabilities
            #
            # NOTE: this assumes that the pilot manages no units, yet.  This will
            # generally be true, as the UM will call this methods before it submits
            # any units.  This will, however, work badly with pilots which are added
            # to more than one UM.  This though holds true for other parts in this
            # code as well, thus we silently ignore this issue for now, and accept
            # this as known limitation....
            self.runqs [pid] = dict()
            self.pilots[pid] = dict()
            self.pilots[pid]['cores']    = pilot.description.cores
            self.pilots[pid]['caps']     = pilot.description.cores
            self.pilots[pid]['state']    = pilot.state
            self.pilots[pid]['resource'] = pilot.resource
            self.pilots[pid]['sandbox']  = pilot.sandbox

            if  OVERSUBSCRIPTION_RATE :
                self.pilots[pid]['caps'] += int(OVERSUBSCRIPTION_RATE * pilot.description.cores / 100.0)

            # make sure we register callback only once per pmgr
            pmgr = pilot.pilot_manager
            if  pmgr not in self.pmgrs :
                self.pmgrs.append (pmgr)
                pmgr.register_callback (self._pilot_state_callback)

            # if we have any pending units, we better serve them now...
            self._reschedule (target_pid=pid)


    # -------------------------------------------------------------------------
    #
    def pilot_removed (self, pid) :

        with self.lock :
            if  not pid in self.pilots :
                raise RuntimeError ('cannot remove unknown pilot (%s)' % pid)

            # NOTE: we don't care if that pilot had any CUs active -- its up to the
            # UM what happens to those.

            del self.pilots[pid]
            # FIXME: how can I *un*register a pilot callback?

            # no need to schedule, really


    # -------------------------------------------------------------------------
    #
    def schedule (self, units) :

        with self.lock :

            # this call really just adds the incoming units to the wait queue and
            # then calls reschedule() to have them picked up.
            for unit in units :

                uid = unit.uid
                
                for pid in self.runqs :
                    if  uid in self.runqs[pid] :
                        raise RuntimeError ('Unit cannot be scheduled twice (%s)' % uid)

                if  uid in self.waitq :
                    raise RuntimeError ('Unit cannot be scheduled twice (%s)' % uid)

                if  unit.state not in [NEW, SCHEDULING, UNSCHEDULED] :
                    # FIXME: clean up, unit should actually not be in
                    #        'SCHEDULING', this is only reached here...
                    raise RuntimeError ('Unit %s not in NEW or UNSCHEDULED state (%s)' % (unit.uid, unit.state))

                self.waitq[uid] = unit

            # lets see what we can do about the known units...
            self._reschedule ()

    
    # -------------------------------------------------------------------------
    #
    def unschedule (self, units) :

        with self.lock :

            # the UM revokes the control over this unit from us...
            for unit in units :

                uid = unit.uid

                for pid in self.runqs :
                    if  uid in self.runqs[pid]  :
                        raise RuntimeError ('cannot unschedule assigned unit (%s)' % uid)

                if  not uid in self.waitq :
                    raise RuntimeError ('cannot remove unknown unit (%s)' % uid)

                # NOTE: we don't care if that pilot had any CUs active -- its up to the
                # UM what happens to those.

                del self.waitq[uid]
                # FIXME: how can I *un*register a pilot callback?
                # FIXME: is this is a race condition with the unit state callback
                #        actions on the queues?


    # -------------------------------------------------------------------------
    #
    def _reschedule (self, target_pid=None, uid=None) :

        with self.lock :

            # dig through the list of waiting CUs, and try to find a pilot for each
            # of them.  This enacts first-come-first-served, but will be unbalanced
            # if the units in the queue are of different sizes.  That problem is
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

            if  target_pid and target_pid not in self.pilots :
                logger.warning ("cannot schedule -- invalid target pilot %s" % target_pid)
                raise RuntimeError ("Invalid pilot (%s)" % target_pid)
                

            schedule           = dict()
            schedule['units']  = dict()
            schedule['pilots'] = self.pilots

            logger.debug ("schedule (%s units waiting)" % len(self.waitq))


            units_to_schedule = list()
            if  uid :

                if  uid not in self.waitq :
                  # self._dump ()
                    logger.warning ("cannot schedule -- unknown unit %s" % uid)
                    raise RuntimeError ("Invalid unit (%s)" % uid)

                units_to_schedule.append (self.waitq[uid])

            else :
                # just copy the whole waitq
                for uid in self.waitq :
                    units_to_schedule.append (self.waitq[uid])


            for unit in units_to_schedule :

                uid = unit.uid
                ud  = unit.description

                # sanity check on unit state
                if  unit.state not in [NEW, SCHEDULING, UNSCHEDULED] :
                    raise RuntimeError ("scheduler requires NEW or UNSCHEDULED units (%s:%s)"\
                                    % (uid, unit.state))

              # logger.debug ("examine unit  %s (%s cores)" % (uid, ud.cores))

                for pid in self.pilots :

                  # logger.debug ("        pilot %s (%s caps, state %s)" \
                  #            % (pid, self.pilots[pid]['state'], self.pilots[pid]['caps']))

                    if  self.pilots[pid]['state'] in [ACTIVE] :

                        if  ud.cores <= self.pilots[pid]['caps'] :
                    
                          # logger.debug ("        unit  %s fits on pilot %s" % (uid, pid))

                            self.pilots[pid]['caps'] -= ud.cores
                            schedule['units'][unit]   = pid

                            # scheduled units are removed from the waitq
                            del self.waitq[uid]
                            self.runqs[pid][uid] = unit
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

