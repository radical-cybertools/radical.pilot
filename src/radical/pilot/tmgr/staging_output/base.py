
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from ... import utils as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's TMGR staging output types
RP_USO_NAME_DEFAULT = "DEFAULT"


# ------------------------------------------------------------------------------
#
class TMGRStagingOutputComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.staging.output.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg.get('tmgr_staging_output_component', RP_USO_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != TMGRStagingOutputComponent:
            raise TypeError("Factory only available to base class!")

        from .default import Default

        try:
            impl = {
                RP_USO_NAME_DEFAULT: Default
            }[name]
            return impl(cfg, session)

        except KeyError as e:
            raise ValueError("TMGRStagingOutputComponent '%s' unknown" % name) \
                from e


# ------------------------------------------------------------------------------

