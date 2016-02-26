
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import threading

import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import UMGRSchedulingComponent


# ==============================================================================
#
class RoundRobin(UMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.pilots = None

        UMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._wait_pool = list()             # set of unscheduled units
        self._wait_lock = threading.RLock()  # look on the above set

        self._idx = 0


    # --------------------------------------------------------------------------
    #
    def add_pilot(self, pid):

        # a pilot just got added.  If we did not have any pilot before, we might
        # have units in the wait queue waiting -- now is a good time to take
        # care of those!
        with self._wait_lock:
            for unit in self._wait_pool:
                unit['pilot'] = pid
                self.advance(unit, rps.UMGR_STAGING_INPUT_PENDING, 
                        publish=True, push=False)

            # all units are scheduled -- empty the wait pool
            self._wait_pool = list()


    # --------------------------------------------------------------------------
    #
    def work(self, unit):

        self.advance(unit, rps.UMGR_SCHEDULING, publish=True, push=False)

        uid = unit['uid']

        with self._pilots_lock:

            # collect all pilots we know about
            pids = list()
            for pid in self._pilots.keys():

                # we need not only an added pilot, we also need one which we 
                # can inspect
                if self._pilots[pid]['thing']:
                    pids.append(pid)

            if not len(pids):

                # no pilot is active, yet -- we add to the wait queue
                with self._wait_lock:
                    self._prof.prof('wait', uid=uid)
                    self._wait_pool.append(unit)

            else:
                # we have active pilots: use them!

                if  self._idx >= len(pids) : 
                    self._idx = 0

                pid = pids[self._idx]

                # we assign the unit to the pilot.
                # Its a good opportunity to also dig out the pilot sandbox and
                # attach it to the unit -- even though this is semantically not
                # relevant here.
                unit['pilot']         = pid
                unit['pilot_sandbox'] = self._pilots[pid]['thing']['sandbox']
                unit['sandbox']       = "%s/%s" % (unit['pilot_sandbox'], uid)

            # we need to push 'pilot' to the db, otherwise the agent will never
            # pick up the unit
            unit['$set'] = ['pilot']
            self.advance(unit, rps.UMGR_STAGING_INPUT_PENDING, 
                    publish=True, push=True)
        

    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):

        pass


# ------------------------------------------------------------------------------

