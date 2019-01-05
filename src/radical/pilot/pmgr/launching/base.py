
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


from ... import utils     as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's pmgr launching types
RP_PL_NAME_SINGLE  = "single"
RP_PL_NAME_BULK    = "bulk"
RP_PL_NAME_DEFAULT = RP_PL_NAME_SINGLE


# ------------------------------------------------------------------------------
#
class PMGRLaunchingComponent(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launcher.
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg.get('type', RP_PL_NAME_DEFAULT)

        # Make sure that we are the base-class!
        if cls != PMGRLaunchingComponent:
            raise TypeError("Factory only available to base class!")

        from .bulk   import Bulk
        from .single import Single

        impls = {RP_PL_NAME_BULK  : Bulk,
                 RP_PL_NAME_SINGLE: Single
        }

        if name not in impls:
            raise ValueError("PMGR Launcher '%s' unknown" % name)

        impl = impls[name]

        return impl(cfg, session)



# ------------------------------------------------------------------------------

