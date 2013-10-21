

import description as descr
import attributes  as att
import sinon._api   as sa


# ------------------------------------------------------------------------------
#
class ComputeUnitDescription (descr.Description, sa.ComputeUnitDescription) :

    def __init__ (self, vals={}) : 

        descr.Description.__init__ (self, vals)

        self._attributes_i_set  ('dtype', sa.COMPUTE, self._DOWN)

        # register properties with the attribute interface
        # action description
        self._attributes_register  (sa.NAME,              None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.EXECUTABLE,        None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.ARGUMENTS,         None, att.STRING, att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.ENVIRONMENT,       None, att.STRING, att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.CLEANUP,           None, att.BOOL,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.START_TIME,        None, att.TIME,   att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.RUN_TIME,          None, att.TIME,   att.SCALAR, att.WRITEABLE)

        # I/O
        self._attributes_register  (sa.WORKING_DIRECTORY, None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.INPUT,             None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.OUTPUT,            None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.ERROR,             None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.FILE_TRANSFER,     None, att.STRING, att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.INPUT_DATA,        None, att.STRING, att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.OUTPUT_DATA,       None, att.STRING, att.VECTOR, att.WRITEABLE)

        # parallelism
        self._attributes_register  (sa.SPMD_VARIATION,    None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.SLOTS,             None, att.INT,    att.SCALAR, att.WRITEABLE)

        # resource requirements
        self._attributes_register  (sa.CPU_ARCHITECTURE,  None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.OPERATING_SYSTEM,  None, att.STRING, att.SCALAR, att.WRITEABLE)
        self._attributes_register  (sa.MEMORY,            None, att.INT,    att.SCALAR, att.WRITEABLE)

        # dependencies
        self._attributes_register  (sa.RUN_AFTER,         None, att.STRING, att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.START_AFTER,       None, att.STRING, att.VECTOR, att.WRITEABLE)
        self._attributes_register  (sa.CONCURRENT_WITH,   None, att.STRING, att.VECTOR, att.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

