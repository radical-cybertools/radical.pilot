

import attributes as att
import sinon._api  as sa


# ------------------------------------------------------------------------------
#
class Description (att.Attributes, sa.Description) :
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals=None) :

        att.Attributes.__init__ (self, vals)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register    ('dtype',  sa.UNKNOWN, att.ENUM, att.SCALAR, att.READONLY)
        self._attributes_set_enums   ('dtype', [sa.UNKNOWN, sa.COMPUTE, sa.DATA])

# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

