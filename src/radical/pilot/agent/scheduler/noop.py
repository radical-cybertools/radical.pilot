
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
    # FIXME: this should not be overloaded here, but in the base class
    #
    def finalize_child(self):

        # make sure that parent finalizers are called
        super(Noop, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, unit):

        # signal success
        return True


# ------------------------------------------------------------------------------

