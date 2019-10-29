
__copyright__ = "Copyright 2014-2016, http://radical.rutgers.edu"
__license__   = "MIT"


from ..  import utils     as rpu


# ------------------------------------------------------------------------------
#
class Agent_n(rpu.Worker):

    # This is a sub-agent.  It does not do much apart from starting
    # agent components and watching them, which is all taken care of in the
    # `Worker` base class (or rather in the `Component` base class of `Worker`).

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        rpu.Worker.__init__(self, cfg, session)


# ------------------------------------------------------------------------------

