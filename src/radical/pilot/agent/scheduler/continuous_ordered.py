
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import copy

import radical.utils as ru

from .continuous import Continuous

from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
#
# This is a simple extension of the Continuous scheduler which evaluates the
# `order` tag of arriving units, which is expected to have the form
#
#   order : {'ns'   : <string>,
#            'order': <int>,
#            'size' : <int>}
#
# where 'ns' is a namespace, 'order' is an integer defining the order of bag of
# tasks in that namespace, and 'size' is the number of tasks in that bag.  The
# semantics of the scheduler is that, for any given namespace, a BoT with order
# 'n' will only be executed after 'size' tasks of the BoT with order 'n-1' have
# been executed.  The first BoT is expected to have order '0'.
#
# The dominant use case for this scheduler is the execution of pipeline stages,
# where one stage needs to be completed before units from the next stage can be
# considered for scheduling.
#
# FIXME: - failed units cannot yet be recognized
#
class ContinuousOrdered(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        Continuous.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        Continuous._configure(self)

        # This scheduler will wait for state updates, and will consider a unit
        # `done` once it reaches a trigger state.  When a state update is found
        # which shows that the units reached that state, it is marked as 'done'
        # in the respective order of its namespace.
        #
        self._trigger_state = rps.UMGR_STAGING_OUTPUT_PENDING
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_cb)

        # a namespace entry will look like this:
        #
        #   { 'current' : 0,   # BoT currently operated on - starts with '0'
        #     0 :              # sequential BoT numbering  `n`
        #     {'size': 128,    # number of units to expect   `max`
        #      'uids': [...]}, # ids    of units to be scheduled
        #      'done': [...]}, # ids    of units in trigger state
        #     },
        #     ...
        #   }
        #
        # prepare an initial entry for each ns which ensures that BOT #0 is
        # runnable once it arrives.

        self._lock       = ru.RLock()   # lock on the ns
        self._units      = dict()       # unit registry (we use uids otherwise)
        self._unordered  = list()       # IDs of units which are not ordered
        self._ns         = dict()       # nothing has run, yet

        self._ns_init    = {'current' : 0}
        self._order_init = {'size'    : 0,
                            'uids'    : list(),
                            'done'    : list()}


    # --------------------------------------------------------------------------
    # overload the main method from the base class
    def _schedule_units(self, units):

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_SCHEDULING, publish=True, push=False)

        with self._lock:

            # cache ID int to avoid repeated parsing
            for unit in units:

                uid       = unit['uid']
                descr     = unit['description']
                order_tag = descr.get('tags', {}).get('order')

                # units w/o order info are handled as usual, and we don't keep
                # any infos around
                if not order_tag:
                  # self._log.debug('no tags for %s', uid)
                    self._unordered.append(unit)
                    continue

                # this uniit wants to be ordered - keep it in our registry
                assert(uid not in self._units), 'duplicated unit %s' % uid
                self._units[uid] = unit

                ns    = order_tag['ns']
                order = order_tag['order']
                size  = order_tag['size']

              # self._log.debug('tags %s: %s : %d : %d', uid, ns, order, size)
                # initiate ns if needed
                if ns not in self._ns:
                    self._ns[ns] = copy.deepcopy(self._ns_init)

                # initiate order if needed
                if order not in self._ns[ns]:
                    self._ns[ns][order] = copy.deepcopy(self._order_init)
                    self._ns[ns][order]['size'] = size

                # ensure that order information are consistent
                assert(size == self._ns[ns][order]['size']), \
                       'inconsistent order size'

                # add unit to order
                self._ns[ns][order]['uids'].append(uid)

        # try to schedule known units
        self._try_schedule()

        return True


    # --------------------------------------------------------------------------
    def _try_schedule(self):
        '''
        Schedule all units in self._unordered.  Then for all name spaces,
        check if their `current` order has units to schedule.  If not and
        we see `size` units are `done`, consider the order completed and go
        to the next one.  Break once we find a BoT which is not completely
        schedulable, either because we did not yet get all its units, or
        because we run out of resources to place those units.
        '''

      # self._log.debug('try schedule')
        scheduled = list()  # list of scheduled units

        # FIXME: this lock is very aggressive, it should not be held over
        #        the scheduling algorithm's activity.
        # first schedule unordered units (
        with self._lock:

            keep = list()
            for unit in self._unordered:

                # attempt to schedule this unit (use continuous algorithm)
                if Continuous._try_allocation(self, unit):

                    # success - keep it and try the next one
                    scheduled.append(unit)

                else:
                    # failure - keep unit around
                    keep.append(unit)

            # keep only unscheduleed units
            self._unordered = keep


        # FIXME: this lock is very aggressive, it should not be held over
        #        the scheduling algorithm's activity.
        with self._lock:

            # now check all namespaces for eligibility
            for ns in self._ns:

                # seek next eligible order - start with current one
                current = self._ns[ns]['current']
                while True:

                    # any information available for the current order?
                    if current not in self._ns[ns]:
                        break

                    order = self._ns[ns][current]

                    # is this order complete?  If so, advance and go to next
                    if order['size'] == len(order['done']):
                      # self._log.debug('order %s [%s] completed', ns, current)
                        current += 1
                        order['current'] = current
                        continue

                    # otherwise we found the order we want to handle
                    # try to schedule the units in this order
                    keep = list()
                    for uid in order['uids']:

                        unit = self._units[uid]

                        # attempt to schedule this unit with the continuous alg
                        if Continuous._try_allocation(self, unit):

                            # success - keep it and try the next one
                            scheduled.append(unit)

                        else:
                            # failure - keep unit around
                            keep.append(uid)

                    # only keep unscheduled units around
                    order['uids'] = keep

                    # we always break after scheduling units from an order.
                    # Either we are out of resources, then we have to try again
                    # later,  or we are out of units, and could switch to the
                    # next order -- but that would require state updates from
                    # the units just submitted, so we break anyway
                    break


        # advance all scheduled units and push them out
        if scheduled:
            self.advance(scheduled, rps.AGENT_EXECUTING_PENDING,
                         publish=True, push=True)

      # self._log.debug('dump')
      # self._log.debug(pprint.pformat(self._ns))


    # --------------------------------------------------------------------------
    #
    def schedule_cb(self, topic, msg):
        '''
        This cb gets triggered after some units got unscheduled, ie. their
        resources have been freed.  We attempt a new round of scheduling at that
        point.
        '''
        self._try_schedule()

        # keep the cb registered
        return True


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, topic, msg):
        '''
        Get state updates, and trigger a new schedule attempt if any unit
        previously scheduled by us reaches the trigger state.
        '''

        cmd    = msg['cmd']
        things = msg['arg']

        if cmd not in ['update']:
            self._log.info('ignore cmd %s', cmd)
            return True

        if not isinstance(things, list):
            things = [things]

        trigger = False
        for thing in things:

            uid   = thing['uid']
            ttype = thing['type']
            state = thing['state']

            if ttype != 'unit':
                continue

            # FIXME: this could be optimized by also checking for later states
            if not state or state != self._trigger_state:
                continue

            with self._lock:
                if uid not in self._units:
                    # unknown unit
                    continue

            # get and parse order tag.  We don't need to repeat checks
            order_tag = thing['description']['tags']['order']
            ns    = order_tag['ns']
            order = order_tag['order']
          # size  = order_tag['size']

            with self._lock:
                self._ns[ns][order]['done'].append(uid)

            trigger = True


        # did any units trigger a reschedule?
        if trigger:
            self._try_schedule()

        return True


# ------------------------------------------------------------------------------

