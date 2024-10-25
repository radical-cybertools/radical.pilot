
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
class AgentResolvingComponent(rpu.AgentComponent):
    '''
    An RP agent resolver will check if tasks are eligible to run.  It can check,
    for example, if data equirements are fullfilled, if the task's environment
    is prepared, if task dependencies are resolved, etc.

    This is the agent scheduler base class.  It provides the framework for
    implementing diverse resolution algorithms, tailored toward specific
    workload types, resource configurations, etc.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate instance for the scheduler.
    #
    @classmethod
    def create(cls, cfg, session):

        # make sure that we are the base-class!
        if cls != AgentResolvingComponent:
            raise TypeError("Resolver Factory only available to base class!")

        name = session.rcfg.agent_resolver

        from .env_prep import EnvPrep

        impl = {
            RESOLVER_ENV : EnvPrep,
        }

        if name not in impl:
            raise ValueError('Resolver %s unknown' % name)

        return impl[name](cfg, session)


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
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_RESOLVING, publish=True, push=False)

        self._work(tasks)

    # --------------------------------------------------------------------------
    #
    def _work(self, tasks):

        # this needs to be overloaded by the inheriting implementation
        raise NotImplementedError('work() is not implemented')


# ------------------------------------------------------------------------------

