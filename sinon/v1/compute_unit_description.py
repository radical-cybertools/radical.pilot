

import sinon.api       as sa
import sinon
from   description import Description
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class ComputeUnitDescription (Description, sa.ComputeUnitDescription) :

    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)

        self._attributes_i_set  ('dtype', sinon.COMPUTE, self._DOWN)

        # register properties with the attribute interface
        # action description
        self._attributes_register  (NAME,              None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (EXECUTABLE,        None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (ARGUMENTS,         None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (ENVIRONMENT,       None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (CLEANUP,           None, BOOL,   SCALAR, WRITEABLE)
        self._attributes_register  (START_TIME,        None, TIME,   SCALAR, WRITEABLE)
        self._attributes_register  (RUN_TIME,          None, TIME,   SCALAR, WRITEABLE)

        # I/O
        self._attributes_register  (WORKING_DIRECTORY, None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (INPUT,             None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (OUTPUT,            None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (ERROR,             None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (FILE_TRANSFER,     None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (INPUT_DATA,        None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (OUTPUT_DATA,       None, STRING, VECTOR, WRITEABLE)

        # parallelism
        self._attributes_register  (SPMD_VARIATION,    None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (SLOTS,             None, INT,    SCALAR, WRITEABLE)

        # resource requirements
        self._attributes_register  (CPU_ARCHITECTURE,  None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (OPERATING_SYSTEM,  None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (MEMORY,            None, INT,    SCALAR, WRITEABLE)

        # dependencies
        self._attributes_register  (RUN_AFTER,         None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (START_AFTER,       None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (CONCURRENT_WITH,   None, STRING, VECTOR, WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

