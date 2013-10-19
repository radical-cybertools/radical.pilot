

import sinon.api       as sa
import sinon
from   attributes  import *


# ------------------------------------------------------------------------------
#
class Description (Attributes, sa.Description) :
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals=None) :

        Attributes.__init__ (self, vals)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register    ('dtype', sinon.UNKNOWN, ENUM, SCALAR, READONLY)
        self._attributes_set_enums   ('dtype', [sinon.UNKNOWN, 
                                                sinon.COMPUTE,
                                                sinon.DATA])

# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

