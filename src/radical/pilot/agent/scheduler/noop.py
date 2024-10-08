
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"


from .base import AgentSchedulingComponent


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


    # --------------------------------------------------------------------------
    #
    def schedule_task(self, task):

        # this abstract method is not used in this implementation
        return None, None


    # --------------------------------------------------------------------------
    #
    def unschedule_task(self, task):

        # this abstract method is not used in this implementation
        pass


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, task):

        # signal success
        return True


# ------------------------------------------------------------------------------

