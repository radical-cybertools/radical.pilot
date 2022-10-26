
__copyright__ = 'Copyright 2022-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
#
# 'enum' for RPs's pilot resolver types
#
RESOLVER_ENV = "RESOLVER_ENV"  # ensure task environment is up

# ------------------------------------------------------------------------------
#
# An RP agent resolver will check if tasks are eligible to run.  It can check,
# for example, if data equirements are fullfilled, if the task's environment is
# prepared, if task dependencies are resolved, etc.
#
# This is the agent scheduler base class.  It provides the framework for
# implementing diverse resolution algorithms, tailored toward specific workload
# types, resource configurations, etc.


# ------------------------------------------------------------------------------
#
class AgentResolvingComponent(rpu.Component):

    def __init__(self, cfg, session):

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize()` will be called
    # before control is given to the component's main loop.
    #
    def initialize(self):

        # register task input and output channels
        self.register_input(rps.AGENT_RESOLVING_PENDING,
                            rpc.AGENT_RESOLVING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate instance for the scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # make sure that we are the base-class!
        if cls != AgentResolvingComponent:
            raise TypeError("Resolver Factory only available to base class!")

        name = cfg['resolver']

        from .env_prep import EnvPrep

        impl = {
            RESOLVER_ENV : EnvPrep,
        }

        if name not in impl:
            raise ValueError('Scheduler %s unknown' % name)

        return impl[name](cfg, session)


# ------------------------------------------------------------------------------

