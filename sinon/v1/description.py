

import sinon.api       as sa
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


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

