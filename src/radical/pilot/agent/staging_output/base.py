
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from ... import utils     as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's agent staging output types
RP_ASI_NAME_DEFAULT = "DEFAULT"


# ------------------------------------------------------------------------------
#
class AgentStagingOutputComponent(rpu.AgentComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.staging.output.%(counter)s',
                                   ru.ID_CUSTOM)

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg.get('agent_staging_output_component', RP_ASI_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != AgentStagingOutputComponent:
            raise TypeError("Factory only available to base class!")

        from .default import Default

        impl = {
            RP_ASI_NAME_DEFAULT: Default
        }

        if name not in impl:
            raise ValueError('AgentStagingOutputComponent %s unknown' % name)

        return impl[name](cfg, session)


# ------------------------------------------------------------------------------

