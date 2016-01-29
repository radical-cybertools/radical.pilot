
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
    def __init__(self, cfg):

        self.pilots = None

        UMGRSchedulingComponent.__init__(self, cfg)


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
            for cu in self._wait_pool:
                cu['pilot'] = pid
                self.advance(cu, rps.UMGR_STAGING_INPUT_PENDING, publish=True, push=False)

            # all units are scheduled -- empty the wait pool
            self._wait_pool = list()


    # --------------------------------------------------------------------------
    #
    def work(self, cu):

        with self._pilots_lock:
            pids = self._pilots.keys()

        if not len(pids):

            # no pilot is active, yet -- we add to the wait queue
            with self._wait_lock:
                self._wait_pool.append(cu)

        else:
            # we have active pilots: use them!

            if  self._idx >= len(pids) : 
                self._idx = 0

            # this is what we consider scheduling :P
            cu['pilot'] = pids[self._idx]

            self.advance(cu, rps.UMGR_STAGING_INPUT_PENDING, publish=True, push=False)
        

    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):

        pass


# ------------------------------------------------------------------------------

