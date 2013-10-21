

import description as descr
import attributes  as att
import sinon.api   as sa


# ------------------------------------------------------------------------------
#

class ComputePilotDescription (descr.Description, sa.ComputePilotDescription) :

    def __init__ (self, vals={}) : 

        descr.Description.__init__ (self, vals)

        # register properties with the attribute interface
        # runtime properties
        self._attributes_register  (sa.START_TIME,        None, att.TIME,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.RUN_TIME,          None, att.TIME,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.CLEANUP,           None, att.BOOL,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.PROJECT,           None, att.STRING, att.SCALAR, att.WRITEABLE)

        # i/o
        self._attributes_register  (sa.WORKING_DIRECTORY, None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.OUTPUT,            None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.ERROR,             None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.FILE_TRANSFER,     None, att.STRING, att.VECTOR, att.WRITEABLE)

        # resource requirements
        self._attributes_register  (sa.RESOURCE,          None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.SLOTS,             None, att.INT,    att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.SPMD_VARIATION,    None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.CANDIDATE_HOSTS,   None, att.INT,    att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.CPU_ARCHITECTURE,  None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.OPERATING_SYSTEM,  None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.MEMORY,            None, att.INT,    att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.QUEUE,             None, att.STRING, att.SCALAR, att.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

