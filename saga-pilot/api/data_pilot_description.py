

from constants   import *
from description import Description


# ------------------------------------------------------------------------------
#

class DataPilotDescription (Description) :
    """
    resource_url        # The URL of the service endpoint
    size                # Storage size of DP (in bytes)

    # AM: lifetime, resource information, etc.
    """
    def __init__ (self, vals={}) : 

        sa.Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (URL,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (RUN_TIME,  None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

