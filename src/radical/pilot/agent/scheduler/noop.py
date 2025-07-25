
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"

import time

from .base import AgentSchedulingComponent

from ...   import states    as rps
from ...   import constants as rpc


# ------------------------------------------------------------------------------
#
# This is a scheduler which does not schedule, at all.  It leaves all placement
# to executors such as srun, jsrun, aprun etc.
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
class Noop(AgentSchedulingComponent):
    '''
    The Noop scheduler does not perform any placement.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentSchedulingComponent.__init__(self, cfg, session)

        # register task output channels
        self.register_output(rps.AGENT_EXECUTING_PENDING,
                             rpc.AGENT_EXECUTING_QUEUE)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        uids = [task['uid'] for task in tasks]

        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)

        self._prof.prof('schedule_try', uid=uids)
        self._prof.prof('schedule_ok',  uid=uids)

        self.advance(tasks, rps.AGENT_EXECUTING_PENDING, publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def unschedule_task(self, task):

        self._prof.prof('unschedule_ok', uid=task['uid'])


    # --------------------------------------------------------------------------
    #
    def _schedule_tasks(self):

        # we don't need a scheduler proc really, but we let it idle as to not
        # trip the pwatcher
        while True:
            time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        pass


# ------------------------------------------------------------------------------

