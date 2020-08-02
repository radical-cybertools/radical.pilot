
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from ... import utils as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's umgr staging input types
RP_USI_NAME_DEFAULT = "DEFAULT"


# ------------------------------------------------------------------------------
#
class UMGRStagingInputComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.staging.input.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg.get('umgr_staging_input_component', RP_USI_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != UMGRStagingInputComponent:
            raise TypeError("Factory only available to base class!")

        from .default import Default

        try:
            impl = {
                RP_USI_NAME_DEFAULT: Default
            }[name]
            return impl(cfg, session)

        except KeyError as e:
            raise ValueError("UMGRStagingInputComponent '%s' unknown" % name) \
                from e


# ------------------------------------------------------------------------------

