
__copyright__ = 'Copyright 2022-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
#
# 'enum' for RP's agent staging input types
#
RP_ASI_NAME_DEFAULT = "DEFAULT"


# ------------------------------------------------------------------------------
#
class AgentStagingInputComponent(rpu.AgentComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != AgentStagingInputComponent:
            raise TypeError("Factory only available to base class!")

        name = cfg.get('agent_staging_input_component', RP_ASI_NAME_DEFAULT)

        from .default import Default

        impl = {
            RP_ASI_NAME_DEFAULT: Default
        }

        if name not in impl:
            raise ValueError('StagingInput %s unknown' % name)

        return impl[name](cfg, session)


    # --------------------------------------------------------------------------
    #
    # Once the component process is spawned, `initialize()` will be called
    # before control is given to the component's main loop.
    #
    def initialize(self):

        # register task input and output channels
        self.register_input(rps.AGENT_STAGING_INPUT_PENDING,
                            rpc.AGENT_STAGING_INPUT_QUEUE, self.work)

        self.register_output(rps.AGENT_SCHEDULING_PENDING,
                             rpc.AGENT_SCHEDULING_QUEUE)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_STAGING_INPUT, publish=True, push=False)

        self._work(tasks)

    # --------------------------------------------------------------------------
    #
    def _work(self, tasks):

        # this needs to be overloaded by the inheriting implementation
        raise NotImplementedError('work() is not implemented')


# ------------------------------------------------------------------------------

