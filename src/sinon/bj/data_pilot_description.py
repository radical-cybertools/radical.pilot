

import description  as descr
import attributes   as att
import sinon._api   as sa


# ------------------------------------------------------------------------------
#

class DataPilotDescription (descr.Description, sa.DataPilotDescription) :

    def __init__ (self, vals={}) : 

        descr.Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (sa.URL,       None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.RUN_TIME,  None, att.BOOL,   att.SCALAR, att.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

