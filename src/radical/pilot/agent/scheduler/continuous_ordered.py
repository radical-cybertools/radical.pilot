
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
# `order` tag of arriving tasks, which is expected to have the form
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
# where one stage needs to be completed before tasks from the next stage can be
# considered for scheduling.
#
# FIXME: - failed tasks cannot yet be recognized
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

        # This scheduler will wait for state updates, and will consider a task
        # `done` once it reaches a trigger state.  When a state update is found
        # which shows that the tasks reached that state, it is marked as 'done'
        # in the respective order of its namespace.
        #
        self._trigger_state = rps.TMGR_STAGING_OUTPUT_PENDING
        self.register_subscriber(rpc.STATE_PUBSUB, self._state_cb)

        # a namespace entry will look like this:
        #
        #   { 'current' : 0,   # BoT currently operated on - starts with '0'
        #     0 :              # sequential BoT numbering  `n`
        #     {'size': 128,    # number of tasks to expect   `max`
        #      'uids': [...]}, # ids    of tasks to be scheduled
        #      'done': [...]}, # ids    of tasks in trigger state
        #     },
        #     ...
        #   }
        #
        # prepare an initial entry for each ns which ensures that BOT #0 is
        # runnable once it arrives.

        self._lock       = ru.RLock()   # lock on the ns
        self._tasks      = dict()       # task registry (we use uids otherwise)
        self._unordered  = list()       # IDs of tasks which are not ordered
        self._ns         = dict()       # nothing has run, yet

        self._ns_init    = {'current' : 0}
        self._order_init = {'size'    : 0,
                            'uids'    : list(),
                            'done'    : list()}


    # --------------------------------------------------------------------------
    # overload the main method from the base class
    def _schedule_task(self, task):

        self.advance(task, rps.AGENT_SCHEDULING, publish=True, push=False)

        with self._lock:

            # cache ID int to avoid repeated parsing
            uid       = task['uid']
            descr     = task['description']
            order_tag = descr.get('tags', {}).get('order')

            # tasks w/o order info are handled as usual, and we don't keep
            # any infos around
            if not order_tag:
              # self._log.debug('no tags for %s', uid)
                self._unordered.append(task)
                return

            # this uniit wants to be ordered - keep it in our registry
            assert uid not in self._tasks, 'duplicated task %s' % uid
            self._tasks[uid] = task

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
            assert size == self._ns[ns][order]['size'], \
                   'inconsistent order size'

            # add task to order
            self._ns[ns][order]['uids'].append(uid)

        # try to schedule known tasks
        self._try_schedule()

        return


    # --------------------------------------------------------------------------
    def _try_schedule(self):
        '''
        Schedule all tasks in self._unordered.  Then for all name spaces,
        check if their `current` order has tasks to schedule.  If not and
        we see `size` tasks are `done`, consider the order completed and go
        to the next one.  Break once we find a BoT which is not completely
        schedulable, either because we did not yet get all its tasks, or
        because we run out of resources to place those tasks.
        '''

      # self._log.debug('try schedule')
        scheduled = list()  # list of scheduled tasks

        # FIXME: this lock is very aggressive, it should not be held over
        #        the scheduling algorithm's activity.
        # first schedule unordered tasks (
        with self._lock:

            keep = list()
            for task in self._unordered:

                # attempt to schedule this task (use continuous algorithm)
                if Continuous._try_allocation(self, task):

                    # success - keep it and try the next one
                    scheduled.append(task)

                else:
                    # failure - keep task around
                    keep.append(task)

            # keep only unscheduleed tasks
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
                    # try to schedule the tasks in this order
                    keep = list()
                    for uid in order['uids']:

                        task = self._tasks[uid]

                        # attempt to schedule this task with the continuous alg
                        if Continuous._try_allocation(self, task):

                            # success - keep it and try the next one
                            scheduled.append(task)

                        else:
                            # failure - keep task around
                            keep.append(uid)

                    # only keep unscheduled tasks around
                    order['uids'] = keep

                    # we always break after scheduling tasks from an order.
                    # Either we are out of resources, then we have to try again
                    # later,  or we are out of tasks, and could switch to the
                    # next order -- but that would require state updates from
                    # the tasks just submitted, so we break anyway
                    break


        # advance all scheduled tasks and push them out
        if scheduled:
            self.advance(scheduled, rps.AGENT_EXECUTING_PENDING,
                         publish=True, push=True)

      # self._log.debug('dump')
      # self._log.debug(pprint.pformat(self._ns))


    # --------------------------------------------------------------------------
    #
    def schedule_cb(self, topic, msg):
        '''
        This cb gets triggered after some tasks got unscheduled, ie. their
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
        Get state updates, and trigger a new schedule attempt if any task
        previously scheduled by us reaches the trigger state.
        '''

        cmd    = msg.get('cmd')
        things = msg.get('arg')

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

            if ttype != 'task':
                continue

            # FIXME: this could be optimized by also checking for later states
            if not state or state != self._trigger_state:
                continue

            with self._lock:
                if uid not in self._tasks:
                    # unknown task
                    continue

            # get and parse order tag.  We don't need to repeat checks
            order_tag = thing['description']['tags']['order']
            ns    = order_tag['ns']
            order = order_tag['order']
          # size  = order_tag['size']

            with self._lock:
                self._ns[ns][order]['done'].append(uid)

            trigger = True


        # did any tasks trigger a reschedule?
        if trigger:
            self._try_schedule()

        return True


# ------------------------------------------------------------------------------

