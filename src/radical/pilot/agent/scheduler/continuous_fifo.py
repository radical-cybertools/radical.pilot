
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


from .continuous import Continuous


# ------------------------------------------------------------------------------
#
# This is a simle extension of the Continuous scheduler which makes RP behave
# like a FiFo: it will only really attempt to schedule units if all older units
# (units with lower unit ID) have been scheduled.  As such, it relies on unit
# IDs to be of the format `unit.%d'`.
#
class ContinuousFifo(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.slots = None
        self._last = -1   # nothing has run, yet - next up is unit.000000

        Continuous.__init__(self, cfg, session)

        # we want to check all units in the wait pool on open slots
        self._uniform_waitpool = False


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, cu):

        # check if the unit is up for scheduling
        uid = cu['uid']
        idx = int(uid.split('.')[1])

        # Units are assigned uids in the order of unit creation.
        # For FIFO, we only need to make sure that the units are allocated in
        # the order of the uids.

        if idx != self._last + 1:
            # nope - we let this attempt fail
            self._log.debug('defer unit %s [%s]', uid, self._last)
            return False

        # yep: lets try for real
        ret = Continuous._try_allocation(self, cu)

        if ret: 
            # it worked!  Keep the new index
            self._last = idx

        return ret


# ------------------------------------------------------------------------------

