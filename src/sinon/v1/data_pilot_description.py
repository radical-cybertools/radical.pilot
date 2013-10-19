

import sinon.api       as sa
from   description import Description
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#

class DataPilotDescription (Description, sa.DataPilotDescription) :

    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (URL,       None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (RUN_TIME,  None, BOOL,   SCALAR, WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

