
__copyright__ = 'Copyright 2022-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'


import threading as mt

from ... import states as rps

from .base import AgentResolvingComponent

# NOTE: this stub contains a lengthy doc snippet from an early attempt to
#       implement a co-location scheduler, mostly for the purpose of preserving
#       that documentation and the included proposal for tag syntax and
#       semantics.
#
#       The addition of a `resolver` component to the agent allows a simpler
#       approach for co-scheduling:
#
#       - this resolver interprets task tags and collects tasks to be co-located
#         into task sets.
#       - for each task set, it defines the *total* set of resource
#         requirements, thus representing the tasks as a collective set of
#         heterogeneous ranks
#       - the scheduler will schedule these ranks of the task set, handling it
#         as it would a single task
#       - the executor will pick ranks from the task set slots for the
#         individual tasks and execute them


# ------------------------------------------------------------------------------
# The BOT Scheduler schedules tasks just like the continuous scheduler (and in
# fact calls the continuous scheduler to do so), but additionally adds structure
# to the stream of tasks, dividing them into chunks, aka 'bag of tasks' (BoT).
# These bag of tasks can have some additional constraints and relations:
#
#   - BoTs can be ordered, i.e., tasks from a BoT with order `n` are not started
#     before all tasks from all BoTs of order `m` with `m < n`.
#
#   - concurrent: tasks in a BoT can be configured to get started at
#     the same time (*)
#
#   - co-locate: tasks in a BoT can be configured to land on the same
#     set of compute nodes (**)
#
#   - de-locate: tasks in a BoT can be configured to land on a different
#     set of compute nodes (**)
#
# To make use of these facilities, tasks will need to be tagged to belong to
# a certain BoT.  Since the RP agent will receive tasks in a continuous stream,
# the tag information will also have to include the size of the BoT, so that the
# scheduler can judge is a bag is complete.  The tag can further include flags
# to trigger concurrency and/or locality constraints:
#
#    tags = {
#        'bot' : {
#            'bid'       : 'foo',  # mandatory
#            'size'      : 4,      # optional, default: 1
#            'order'     : 2,      # optional, default: None
#            'concurrent': False,  # optional, default: False
#            'co-locate' : False   # optional, default: False
#            'de-locate' : False   # optional, default: False
#        }
#    }
#
#
# Note that the tags for all tasks in a BoT must be consistent - otherwise all
# tasks in that bag are marked as `FAILED`.
#
# Note that a BoT ID can be reused.  For example, 4 tasks can share a BoT ID
# `bot1` of size `2`.  The scheduler will collect 2 tasks and run them.  If it
# encounters the same BoT ID again, it will again collect 2 tasks.  If
# `co-locate` is enabled, then the second batch will run on the same node as the
# first batch.  If the BoT has an order defined, then the first batch will need
# to see at least one BoT for each lower order completed before the BoT is
# eligible.  If a subsequent batch is received for the same BoT ID, then at
# least two batches of the lower order BoT need to run first, etc.
#
#
# (*)  'at the same time': small differences in startup time may occur due to
#      RP agent and HPC system configuration, RP though guarantees that the BoT
#      becomes eligible for execution at the exact same time.
#
# (**) 'set of compute nodes': the current implementation can only handle
#      `co-locate` and `de-locate` for tasks of size up to a single node.
#
#
# Examples:
#
#   task.1  bid=bot1  size=4
#   task.2  bid=bot1  size=4
#   task.3  bid=bot1  size=4
#   task.4  bid=bot1  size=4
#
#   The tasks 1-4 will be scheduled and executed individually - but only become
#   eligible for execution once all 4 tasks arrive in the scheduler.
#
#
#   task.1  bid=bot1  size=2  order=None  concurrent=True  co-locate=True
#   task.2  bid=bot1  size=2  order=None  concurrent=True  co-locate=True
#   task.3  bid=bot2  size=2  order=None  concurrent=True  co-locate=True
#   task.4  bid=bot2  size=2  order=None  concurrent=True  co-locate=True
#
#   tasks 1 and 2 will run concurrently on the same node, tasks 3 and 4 will
#   also run concurrently on one node (possibly at a different time).  The node
#   for the first batch may or may not be the same as for the second batch.
#
#
#   task.1  bid=bot1  size=2
#   task.2  bid=bot1  size=2
#   task.3  bid=bot1  size=2
#   task.4  bid=bot1  size=2
#
#   tasks 1 and 2 will run concurrently on the same node, tasks 3 and 4 will
#   also run concurrently on _the same_ node (possibly at a different time).
#   The node for the first batch may or may not be the same as for the second
#   batch.
#
#
#   task.1  bid=bot1  size=3
#   task.2  bid=bot1  size=3
#   task.3  bid=bot1  size=3
#   task.4  bid=bot1  size=3
#
#   tasks 1 to 3 will run concurrently on the same node, but task 4 will never
#   get scheduled (unless more tasks arrive to complete the batch).
#
#
#   task.1  bid=bot1
#   task.2  bid=bot1
#   task.3  bid=bot1
#   task.4  bid=bot1
#
#   tasks 1 to 4 will land on the same node, possibly at different times.
#
#
# This is a simple extension of the Continuous scheduler which evaluates the
# `colocate` tag of arriving tasks, which is expected to have the form
#   task.1  size=4
#   task.2  size=4
#   task.3  size=4
#   task.4  size=4
#
#   colocate : {'ns'   : <string>,
#               'size' : <int>}
#   tasks 1 to 4 will run concurrently, but possibly on different nodes.
#
#   task.1  size=4 de-locate=True
#   task.2  size=4 de-locate=True
#   task.3  size=4 de-locate=True
#   task.4  size=4 de-locate=True
#
#   tasks 1 to 4 will run concurrently, but guaranteed on different nodes
#   (needs 4 nodes!)
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
# NOTE: tasks exit codes don't influence the scheduling algorithm: subsequent
#       task batches will be scheduled even if the first batch completed with
#       a non=zero exit code.
#
#       If a string is specified instead of a dict, it is interpreted as `node`.
#       If an integer is specified, it is interpreted a batch `size`.
#
#       If `node` is not specified, no node locality is enforced - the
#       algorithm only respects time locality (`size`).
#
#       If `size` is not specified, no time colocation if enforced - the
#       algorithm only respects node locality.  This is the same behaviour as
#       with `size=1`.
#
#
# Implementation:
#
# The scheduler operates on tasks in two distinct stages:
#
#   1 - eligibility: a task becomes eligible for placement when all dependencies
#       are resolved.  Dependencies are :
#       - all bags of lower order are completed (*)
#       - all tasks to co-locate with this task are available
#       - all tasks to de-locate with this task are available
#
#   2 - placement:  a set of tasks which is eligible for placement is scheduled
#       on the candidate nodes
#
#   (*) completed means that the scheduler received a notification that the task
#       entered `UMGR_STAGING_OUTPUT_PENDING` state, implying that task
#       execution completed and that all *local* staging directives have been
#       enacted.
#
#
# When scheduling a set of tasks, the following candidate nodes are considered
# for placement:
#
#   - co-location: the first task in the respective bag is scheduled on *any*
#     node.  Any subsequent task in the same bag will be scheduled on the *same*
#     node as the first task - no other nodes are considered.
#
#   - de-location: the first task is scheduled on *any* node.  The next task in
#     the same bag is scheduled on any node *but* the one the first is placed
#     on, and so on.  If no nodes remain available (BoT size > n_nodes), the
#     task will fail.
#
#   - concurrent: one task is scheduled after the other, according to the
#     constraints above.  If any task in the bag fails to schedule, all
#     previously scheduled tasks will be unscheduled and have to wait.  Tasks
#     are advanced in bulk only after all tasks have been placed successfully.
#
#   - all other tasks (without locality or concurrency constraints) are
#     scheduled individually.
#
#   - concurrent co-location (optimized): the resources for all tasks in the bag
#     are added up, and a single virtual task is scheduled on *any* node.  Once
#     the virtual task is placed, the allocation is again split up between the
#     individual tasks, resulting in their respective placement, and the bag is
#     advanced in bulk.
#
# NOTE: This functionality is dangerously close (and in fact overlapping) with
#       workflow orchstration.  As such, it should actually not live in the
#       scheduler but should move into a separate RCT component.  To cater for
#       that move at a later point, we render the specific code parts as filter
#       whose implementation we should be able to easily extract as needed.  The
#       filter
#
#         - receives a set of tasks
#         - filters tasks which are eligible to run under the given tag
#           constraints
#         - forwards eligible tasks to the scheduler proper
#         - retain non-eligible tasks until new information received via any
#           communication channels indicates that they become eligible to run
#
#       The wait list managed by that filer is different and independent from
#       the wait list maintained in the scheduler where tasks are waiting for
#       required resources to become available).
#

...

# where one stage needs to be completed before tasks from the next stage can be
# considered for scheduling.
#
# NOTE: tasks exit codes don't influence the scheduling algorithm: subsequent
#       task batches will be scheduled even if the first batch completed with
#       a non=zero exit code.
#
#       If `size` is not specified, it defaults to `1` and tasks are run
#       individually in the given order.
#
#       If an integer is specified instead of a dict, it is interpreted a
#       `order` - `ns` is in this case set to `default`, and `size` to `1`.
#
# FIXME: - failed tasks cannot yet be recognized
#


# ------------------------------------------------------------------------------
#
class Tags(AgentResolvingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        super().initialize()

        # keep track of known named envs
        self._named_envs = list()
        self._waitpool   = collections.defaultdict(list)
        self._lock       = mt.Lock()


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):
        '''
        listen on the control channel for raptor queue registration commands
        '''

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'register_named_env':
            if arg:
                pass

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_RESOLVING, publish=True, push=False)

        self.advance(tasks, rps.AGENT_STAGING_INPUT_PENDING,
                     publish=True, push=True)


# ------------------------------------------------------------------------------

