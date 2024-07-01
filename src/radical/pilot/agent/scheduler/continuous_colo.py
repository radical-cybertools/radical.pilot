
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
# `colocate` tag of arriving tasks, which is expected to have the form
#
#   colocate : {'ns'   : <string>,
#               'size' : <int>}
#
# where 'ns' (for namespace) is a bag ID, and 'size' is the number of tasks in
# that bag of tasks that need to land on the same host.  The semantics of the
# scheduler is that, for any given namespace, it will schedule either all tasks
# in that ns at the same time on the same node, or will schedule no task of that
# ns at all.
#
# The dominant use case for this scheduler is the execution of coupled
# applications which exchange data via shared local files or shared memory.
#
# FIXME: - failed tasks cannot yet considered, subsequent tasks in the same ns
#          will be scheduled anyway.
#
class ContinuousColo(Continuous):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        Continuous.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        Continuous._configure(self)

        # a 'bag' entry will look like this:
        #
        #   {
        #      'size': 128,    # number of tasks to expect
        #      'uids': [...]}, # ids    of tasks to be scheduled
        #   }

        self._lock      = ru.RLock()   # lock on the bags
        self._tasks     = dict()       # task registry (we use uids otherwise)
        self._unordered = list()       # IDs of tasks which are not colocated
        self._bags      = dict()       # nothing has run, yet

        self._bag_init  = {'size' : 0,
                           'uids' : list()}


    # --------------------------------------------------------------------------
    # overload the main method from the base class
    def schedule_task(self, task):

        self.advance(task, rps.AGENT_SCHEDULING, publish=True, push=False)

        with self._lock:

            # cache ID int to avoid repeated parsing
            uid      = task['uid']
            descr    = task['description']
            colo_tag = descr.get('tags', {}).get('colocate')

            # tasks w/o order info are handled as usual, and we don't keep
            # any infos around
            if not colo_tag:
              # self._log.debug('no tags for %s', uid)
                self._unordered.append(task)
                return None, None

            # this uniit wants to be ordered - keep it in our registry
            assert uid not in self._tasks, 'duplicated task %s' % uid
            self._tasks[uid] = task

            bag   = colo_tag['bag']
            size  = colo_tag['size']

          # self._log.debug('tags %s: %s : %d', uid, bag, size)

            # initiate bag if needed
            if bag not in self._bags:
                self._bags[bag]         = copy.deepcopy(self._bag_init)
                self._bags[bag]['size'] = size

            else:
                assert size == self._bags[bag]['size'], \
                       'inconsistent bag size'

            # add task to order
            self._bags[bag]['uids'].append(uid)

        # try to schedule known tasks
        self._try_schedule()

        return None, None


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

        self._log.debug('try schedule')
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

            # now check all bags for eligibility, filter scheduled ones
            to_delete = list()
            for bag in self._bags:

                self._log.debug('try bag %s', bag)

                if self._bags[bag]['size'] < len(self._bags[bag]['uids']):
                    raise RuntimeError('inconsistent bag assembly')

                # if bag is complete, try to schedule it
                if self._bags[bag]['size'] == len(self._bags[bag]['uids']):

                    self._log.debug('try bag %s (full)', bag)
                    if self._try_schedule_bag(bag):

                        self._log.debug('try bag %s (placed)', bag)
                        # scheduling works - push tasks out and erase all traces
                        # of the bag (delayed until after iteration)
                        for uid in self._bags[bag]['uids']:

                            scheduled.append(self._tasks[uid])

                        to_delete.append(bag)

            # delete all bags which have been pushed out
            for bag in to_delete:

                del self._bags[bag]


        # advance all scheduled tasks and push them out
        if scheduled:
            self.advance(scheduled, rps.AGENT_EXECUTING_PENDING,
                         publish=True, push=True)

      # self._log.debug('dump')
      # self._log.debug(pprint.pformat(self._bags))


    # --------------------------------------------------------------------------
    #
    def _try_schedule_bag(self, bag):
        '''
        This methods assembles the requiremets of all tasks in a bag into
        a single pseudo-task.  We ask the cont scheduler to schedule that
        pseudo-task for us.  If that works, we disassemble the resulting
        resource slots and assign them to the bag's tasks again, and declare
        success.
        '''

        self._log.debug('try schedule bag %s ', bag)

        tasks  = [self._tasks[uid] for uid in self._bags[bag]['uids']]
        pseudo = copy.deepcopy(tasks[0])

        pseudo['uid'] = 'pseudo.'

        descr = pseudo['description']
        descr['threading_type']   = rpc.POSIX  # force single node
        descr['ranks']            = 1
        descr['cores_per_rank']   = 1
        descr['gpus_per_rank']    = 0.
        descr['gpu_type']         = None

        self._log.debug('try schedule uids  %s ', self._bags[bag]['uids'])
      # self._log.debug('try schedule tasks  %s ', pprint.pformat(tasks))

        for task in tasks:
            td = task['description']
            pseudo['uid'] += task['uid']

            descr['cores_per_rank'] += td['ranks'] * td['cores_per_rank']
            descr['gpus_per_rank']  += td['ranks'] * td['gpus_per_rank']

      # self._log.debug('try schedule pseudo %s ', pprint.pformat(pseudo))

        if not Continuous._try_allocation(self, pseudo):

            # cannot scshedule this pseudo task right now, bag has to wait
            return False

        # we got an allocation for the pseudo task, not dissassemble the slots
        # and assign back to the individual tasks in the bag
        for task in tasks:

            ranks = task['description']['ranks']
            task['slots'] = list()

            for rank in range(ranks):
                task['slots'].append(pseudo['slots'].pop(0))

        return True


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


# ------------------------------------------------------------------------------

