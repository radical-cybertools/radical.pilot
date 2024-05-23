
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from ... import utils as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's tmgr staging input types
RP_USI_NAME_DEFAULT = "DEFAULT"


# ------------------------------------------------------------------------------
#
class TMGRStagingInputComponent(rpu.ClientComponent):

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

        name = cfg.get('tmgr_staging_input_component', RP_USI_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != TMGRStagingInputComponent:
            raise TypeError('Factory only available to base class!')

        from .default import Default

        impl = {
            RP_USI_NAME_DEFAULT: Default
        }

        if name not in impl:
            raise ValueError('StagingIn %s unknown' % name)

        return impl[name](cfg, session)


# ------------------------------------------------------------------------------

