

import sinon.api       as sa
from   description import Description
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class DataUnitDescription (Description, sa.DataUnitDescription) :
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (NAME,              None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (FILE_URLS,         None, DICT,   SCALAR, WRITEABLE)
        self._attributes_register  (LIFETIME,          None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (CLEANUP,           None, BOOL,   SCALAR, WRITEABLE)
        self._attributes_register  (SIZE,              None, INT,    SCALAR, WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

