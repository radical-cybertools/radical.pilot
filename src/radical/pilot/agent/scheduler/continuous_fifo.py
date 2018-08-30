
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import threading as mt

from .continuous import Continuous

from ... import states    as rps


# ------------------------------------------------------------------------------
#
# This is a simple extension of the Continuous scheduler which makes RP behave
# like a FiFo: it will only really attempt to schedule units if all older units
# (units with lower unit ID) have been scheduled.  As such, it relies on unit
# IDs to be of the format `unit.%d'`.
#
class ContinuousFifo(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        Continuous.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        Continuous._configure(self)

        self._last              = -1   # nothing has run, yet
        self._ordered_wait_pool = list()
        self._ordered_wait_lock = mt.RLock()  # look on the above set


    # --------------------------------------------------------------------------
    # overload the main method from the base class
    def work(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_SCHEDULING, publish=True, push=False)

        # add incoming units to the wait list, sort it again by unit ID
        with self._ordered_wait_lock:

            # cache ID int to avoid repeated parsing
            for unit in units:
                if 'serial_id' not in unit:
                    unit['serial_id'] = int(unit['uid'].split('.')[-1])

                assert(unit['serial_id'] > self._last)

            # add units to pool and resort it
            self._ordered_wait_pool += units
            self._ordered_wait_pool.sort(key=lambda x: x['serial_id'])


        # try to schedule from the ordered wait list
        self._try_schedule()


    # --------------------------------------------------------------------------
    def _try_schedule(self):
        '''
        Walk through the ordered list of units and schedule all eligible ones.
        Once done (either all are scheduled or we find one which is not
        eligible, which means that all following ones are not eligible either),
        we break and remove those units from the wait pool.
        '''

        scheduled = list()  # list of scheduled units
        with self._ordered_wait_lock:

            if not self._ordered_wait_pool:
                # nothing to do
                return

            for unit in self._ordered_wait_pool:

                # check if this unit is eligible for scheduling
                if unit['serial_id'] == (self._last + 1):

                    # attempt to schedule this unit (use continuous algorithm)
                    if Continuous._try_allocation(self, unit):

                        # success - keep it and try the next one
                        scheduled.append(unit)
                        self._last += 1

                    else:
                        break  # no resources available - break
                else:
                    break  # unit not eligible - and neither are later ones

            # remove scheduled units from the wait list
            if scheduled:
                n = len(scheduled)
                self._ordered_wait_pool = self._ordered_wait_pool[n:]

        # advance units and push them out
        if scheduled:
            self.advance(scheduled, rps.AGENT_EXECUTING_PENDING, 
                         publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def unschedule_cb(self, topic, msg):
        """
        overload the reschedule trigger callback from base.
        """

        # try to schedule from the ordered wait list
        self._try_schedule()

        return True


# ------------------------------------------------------------------------------

