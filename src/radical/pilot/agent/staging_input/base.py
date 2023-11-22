
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

import radical.utils      as ru

from ... import states    as rps
from ... import constants as rpc
from ... import utils     as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's agent staging input types
RP_ASI_NAME_DEFAULT = "DEFAULT"


# ------------------------------------------------------------------------------
#
class AgentStagingInputComponent(rpu.AgentComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.staging.input.%(counter)s',
                                   ru.ID_CUSTOM)

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg.get('agent_staging_input_component', RP_ASI_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != AgentStagingInputComponent:
            raise TypeError("Factory only available to base class!")

        from .default import Default

        impl = {
            RP_ASI_NAME_DEFAULT: Default
        }

        if name not in impl:
            raise ValueError('AgentStagingInputComponent %s unknown' % name)

        return impl[name](cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._pwd = os.getcwd()

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

