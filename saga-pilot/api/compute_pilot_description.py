

from constants   import *
from description import Description


# ------------------------------------------------------------------------------
#

class ComputePilotDescription (Description) :
    """
    """
    def __init__ (self, vals={}) : 

        sa.Description.__init__ (self, vals)

        # register properties with the attribute interface
        # runtime properties
        self._attributes_register  (START_TIME,        None, sa.TIME,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (RUN_TIME,          None, sa.TIME,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CLEANUP,           None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (PROJECT,           None, sa.STRING, sa.SCALAR, sa.WRITEABLE)

        # i/o
        self._attributes_register  (WORKING_DIRECTORY, None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (OUTPUT,            None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (ERROR,             None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (FILE_TRANSFER,     None, sa.STRING, sa.VECTOR, sa.WRITEABLE)

        # resource requirements
        self._attributes_register  (SLOTS,             None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (SPMD_VARIATION,    None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CANDIDATE_HOSTS,   None, sa.INT,    sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (CPU_ARCHITECTURE,  None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (OPERATING_SYSTEM,  None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (MEMORY,            None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (QUEUE,             None, sa.STRING, sa.SCALAR, sa.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

