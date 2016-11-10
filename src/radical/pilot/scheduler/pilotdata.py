#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.scheduler.PilotDataScheduler
   :platform: Unix
   :synopsis: A multi-pilot, PilotData scheduler.

.. moduleauthor:: Mark Santcroos <mark.santcroos@rutgers.edu>
"""

__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import math
import json
import pprint
import random
import threading

from ..states   import *
from ..utils    import logger
from ..utils    import timestamp
from ..constants import *
from ..exceptions import *

from .interface import Scheduler

import radical.utils as ru

from ..staging_directives import expand_staging_directive

# to reduce roundtrips, we can oversubscribe a pilot, and schedule more units
# than it can immediately execute.  Value is in %.
OVERSUBSCRIPTION_RATE = 0

#
# Constants
#
DIRECTION_INPUT = 'direction_input'
DIRECTION_OUTPUT = 'direction_output'

#
# OSG CONFIG
#
PREFERRED_SES = 'preferred_ses.json'
RELIABILITY_CE2SE = 'reliability_ce2se.json'
PERFORMANCE_CE2SE = 'performance_ce2se.json'


# -----------------------------------------------------------------------------
#
class PilotDataScheduler(Scheduler):
    """

    PilotDataScheduler implements a multi-pilot, backfilling scheduling
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
        self.cb_hist = {}

        self._read_configs()

        # make sure the UM notifies us on all unit state changes
        manager.register_callback (self._unit_state_callback)


    def _read_configs(self):

        osg_config_dir = os.getenv('RADICAL_PILOT_OSG_CONFIG_DIR')

        if not osg_config_dir:
            raise Exception("RADICAL_PILOT_OSG_CONFIG_DIR not set!")

        f = open(os.path.join(osg_config_dir, PREFERRED_SES))
        self._preferred_ses = json.load(f)
        f.close()

        f = open(os.path.join(osg_config_dir, RELIABILITY_CE2SE))
        self._reliable_ses = json.load(f)
        f.close()

        f = open(os.path.join(osg_config_dir, PERFORMANCE_CE2SE))
        self._fast_ses = json.load(f)
        f.close()


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

                logger.info("[SchedulerCallback]: ComputeUnit %s changed to %s" % (uid, state))

                logger.debug("[SchedulerCallback]: unit state callback history: %s" % (self.cb_hist))
                if state == UNSCHEDULED and SCHEDULING in self.cb_hist[uid]:
                    logger.warn("[SchedulerCallback]: ComputeUnit %s with state %s already dealt with." % (uid, state))
                    return
                self.cb_hist[uid].append(state)

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

                    # Get OSG site
                    pilot_info = self._dbs.get_pilots(pilot_ids=pid)
                    if len(pilot_info) != 1:
                        raise Exception("Should not get more (or less) than one entry back here!")

                    # Resource site name
                    try:
                        self.pilots[pid]['osg_resource_name'] = pilot_info[0]['osg_resource_name']
                    except KeyError:
                        self.pilots[pid]['osg_resource_name'] = 'Unknown'

                    # Trigger scheduler
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
            self.pilots[pid]['osg_resource_name'] = None

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

                self.cb_hist[uid] = []
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
    def _select_dp(self, du, direction, cu_resource=None):

        if direction == DIRECTION_INPUT:
            print "Input DU %s selecting from available DP_ID's: %s" % (du.uid, du.pilot_ids)
        else:
            print "Output DU %s selection from DP_ID's: %s" % (du.uid, du.pilot_ids)

        sel = du.description.selection

        print "CU Resource: %s" % cu_resource

        if sel == SELECTION_PREFERRED:
            if cu_resource in self._preferred_ses:
                pref = self._preferred_ses[cu_resource]
                print "Site has a preferred SE: %s" % pref
            else:
                print "Site has no preferred SE."
                pref = None

            if pref:
                for dp_id in du.pilot_ids:
                    # Iterate over all PMGR's to find the DP
                    for pmgr in self.pmgrs:
                        if dp_id in pmgr.list_data_pilots():
                            # Get the DP object
                            dp = pmgr.get_data_pilots(dp_id)

                            if dp.resource.split('.')[1] == pref:
                                print "Preferred SE is in list of replicas: %s" % pref
                                return dp

                print "Preferred SE is not in list of replicas."

            #
            # Fallback to random selection
            #

        elif sel == SELECTION_FAST:
            if not du.description.size:
                raise Exception("Size not specified in DU %s." % du.description)

            def get_fast(matrix, source, size):

                print "Finding fastest SE's for source: %s for size: %s" % (source, size)

                try:
                    targets = matrix[source]
                except KeyError:
                    return None
                # JSON file has string key identifiers
                # Throwing out unknown values
                results = {x: targets[x][str(size)] for x in targets if targets[x][str(size)] != -1}
                sites = sorted(results, key=results.get)
                return sites

            fast_sites_ordered = get_fast(self._fast_ses, cu_resource, du.description.size)

            if fast_sites_ordered:
                dps = []
                for dp_id in du.pilot_ids:
                    for pmgr in self.pmgrs:
                        if dp_id in pmgr.list_data_pilots():
                            # Get the DP object
                            dp = pmgr.get_data_pilots(dp_id)
                            dps.append(dp)

                for site in fast_sites_ordered:
                    print "Checking if fast site: %s is in the list of pilots ..." % site
                    for dp in dps:
                        if dp.resource.split('.')[1] == site:
                            print "Selecting DP Resource: %s" % dp.resource
                            return dp
                        else:
                            print "Not Selecting DP Resource: %s" % dp.resource

            #
            # Fallback to random selection
            #


        elif sel == SELECTION_SLOW:
            if not du.description.size:
                raise Exception("Size not specified in DU %s." % du.description)

            def get_slow(matrix, source, size):

                print "Finding slowest SE's for source: %s for size: %s" % (source, size)

                try:
                    targets = matrix[source]
                except KeyError:
                    return None
                # JSON file has string key identifiers
                # Throwing out unknown values
                results = {x: targets[x][str(size)] for x in targets if targets[x][str(size)] != -1}
                sites = sorted(results, key=results.get, reverse=True)
                return sites

            slow_sites_ordered = get_slow(self._fast_ses, cu_resource, du.description.size)

            if slow_sites_ordered:
                dps = []
                for dp_id in du.pilot_ids:
                    for pmgr in self.pmgrs:
                        if dp_id in pmgr.list_data_pilots():
                            # Get the DP object
                            dp = pmgr.get_data_pilots(dp_id)
                            dps.append(dp)

                for site in slow_sites_ordered:
                    print "Checking if slow site: %s is in the list of pilots ..." % site
                    for dp in dps:
                        if dp.resource.split('.')[1] == site:
                            print "Selecting DP Resource: %s" % dp.resource
                            return dp
                        else:
                            print "Not Selecting DP Resource: %s" % dp.resource

            #
            # Fallback to random selection
            #

        elif sel == SELECTION_RELIABLE:
            if not du.description.size:
                raise Exception("Size not specified in DU")

            def get_reliable(matrix, source, size):

                print "Finding reliable SE's for source: %s for size: %s" % (source, size)

                targets = matrix[source]
                # JSON file has string key identifiers
                results = {x: targets[x][str(size)] for x in targets}
                sites = sorted(results, key=results.get, reverse=True)
                return sites

            reliable_sites_ordered = get_reliable(self._reliable_ses, cu_resource, du.description.size)

            dps = []
            for dp_id in du.pilot_ids:
                for pmgr in self.pmgrs:
                    if dp_id in pmgr.list_data_pilots():
                        # Get the DP object
                        dp = pmgr.get_data_pilots(dp_id)
                        dps.append(dp)

            for site in reliable_sites_ordered:
                print "Checking if reliable site: %s is in the list of pilots ..." % site
                for dp in dps:
                    if dp.resource.split('.')[1] == site:
                        print "Selecting DP Resource: %s" % dp.resource
                        return dp
                    else:
                        print "Not Selecting DP Resource: %s" % dp.resource

            #
            # Fallback to random selection
            #

        elif sel == SELECTION_RANDOM:
            # Fallback
            pass

        else:
            raise Exception("Selection mechanism '%s' unknown!" % sel)

        #
        # Pick a random DP this DU is available on
        #
        print "Going to pick random DP ..."
        dp_id = random.choice(du.pilot_ids)
        print "Using DU: %s on DP_ID: %s" % (du.uid, dp_id)
        # Iterate over all PMGR's to find the DP
        for pmgr in self.pmgrs:
            if dp_id in pmgr.list_data_pilots():
                print "DP: %s is on PMGR: %s" % (dp_id, pmgr.uid)
                # Get the DP object
                dp = pmgr.get_data_pilots(dp_id)
                print "DP Resource: %s" % dp.resource

                return dp

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

                for pid in self.pilots :

                    if self.pilots[pid]['state'] in [ACTIVE]:

                        if ud.candidate_hosts and \
                           self.pilots[pid]['osg_resource_name'] not in ud.candidate_hosts:
                            continue

                        if  ud.cores <= self.pilots[pid]['caps'] :

                            self.pilots[pid]['caps'] -= ud.cores
                            schedule['units'][unit]   = pid

                            # Populate Pilot Data entries
                            print "cu scheduler - du(s): %s" % ud.input_data

                            # Get all the DU's for the DU-ID's provided in ud.input_data
                            if ud.input_data:
                                dus = self.manager.get_data_units(ud.input_data)
                            else:
                                dus = []

                            # Iterate over all input DU's
                            for du in dus:

                                dp = self._select_dp(du, DIRECTION_INPUT, self.pilots[pid]['osg_resource_name'])
                                ep = dp._resource_config['filesystem_endpoint']
                                if du._existing:
                                    sd = expand_staging_directive(['%s/%s' % (ep, fu) for fu in du.description.files])
                                else:
                                    sd = expand_staging_directive(['%s/tmp/%s/%s/%s' % (ep, self.session.uid, du.uid, fu) for fu in du.description.files])

                                # In case the source is a file://, change the action to copy
                                for s in sd:
                                    if ru.Url(s['source']).schema == 'file':
                                        s['action'] = COPY

                                if not unit.description.input_staging:
                                    unit.description.input_staging = sd
                                else:
                                    unit.description.input_staging.extend(sd)

                            # Get all the DU's for the DU-ID's provided in ud.output_data
                            if ud.output_data:
                                dus = self.manager.get_data_units(ud.output_data)
                            else:
                                dus = []

                            # Iterate over all output DU's
                            for du in dus:
                                dp = self._select_dp(du, DIRECTION_OUTPUT, self.pilots[pid]['osg_resource_name'])
                                du.pilot_ids = [dp.uid]
                                ep = dp._resource_config['filesystem_endpoint']
                                sd = expand_staging_directive(['%s > %s/tmp/%s/%s/%s' % (fu, ep, self.session.uid, du.uid, fu) for fu in du.description.files])

                                # In case the target is a file://, change the action to copy
                                for s in sd:
                                    if ru.Url(s['target']).schema == 'file':
                                        s['action'] = COPY

                                if not unit.description.output_staging:
                                    unit.description.output_staging = sd
                                else:
                                    unit.description.output_staging.extend(sd)

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
