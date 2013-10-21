

import description  as descr
import attributes   as att
import sinon._api   as sa


# ------------------------------------------------------------------------------
#
class DataUnitDescription (descr.Description, sa.DataUnitDescription) :
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals={}) : 

        descr.Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (sa.NAME,      None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.FILE_URLS, None, att.DICT,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.LIFETIME,  None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.CLEANUP,   None, att.BOOL,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.SIZE,      None, att.INT,    att.SCALAR, att.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

