

import sinon.api       as sa
from   description import Description
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#

class ComputePilotDescription (Description, sa.ComputePilotDescription) :

    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)

        # register properties with the attribute interface
        # runtime properties
        self._attributes_register  (START_TIME,        None, TIME,   SCALAR, WRITEABLE)
        self._attributes_register  (RUN_TIME,          None, TIME,   SCALAR, WRITEABLE)
        self._attributes_register  (CLEANUP,           None, BOOL,   SCALAR, WRITEABLE)
        self._attributes_register  (PROJECT,           None, STRING, SCALAR, WRITEABLE)

        # i/o
        self._attributes_register  (WORKING_DIRECTORY, None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (OUTPUT,            None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (ERROR,             None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (FILE_TRANSFER,     None, STRING, VECTOR, WRITEABLE)

        # resource requirements
        self._attributes_register  (SLOTS,             None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (SPMD_VARIATION,    None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (CANDIDATE_HOSTS,   None, INT,    VECTOR, WRITEABLE)
        self._attributes_register  (CPU_ARCHITECTURE,  None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (OPERATING_SYSTEM,  None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (MEMORY,            None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (QUEUE,             None, STRING, SCALAR, WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

