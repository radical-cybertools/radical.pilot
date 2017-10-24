
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc


# ------------------------------------------------------------------------------
# 'enum' for RP's pmgr launching types
RP_UL_NAME_DEFAULT = "DEFAULT"


# ==============================================================================
#
class PMGRLaunchingComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.launching.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)

        self._pmgr = self._owner

    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launcher.
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg.get('pmgr_launching_component', RP_UL_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != PMGRLaunchingComponent:
            raise TypeError("Factory only available to base class!")

        from .default import Default

        try:
            impl = {
                RP_UL_NAME_DEFAULT: Default
            }[name]

            impl = impl(cfg, session)
            return impl

        except KeyError:
            raise ValueError("PMGRLaunchingComponent '%s' unknown or defunct" % name)


# ------------------------------------------------------------------------------

