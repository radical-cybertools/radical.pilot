

from constants   import *
from description import Description


# ------------------------------------------------------------------------------
#
class ComputeUnitDescription (Description) :
    """
    # Action description
    'name',                 # Non-unique name/label of CU.
    'executable',           # The "action" to execute
    'arguments',            # Arguments to the "action"
    'environment',          # "environment" settings for the "action"
    'cleanup',              # cleanup after the CU has finished
    'start_time',           # When should the CU start
    'run_time',             # When is the CU expected to finish

    # I/O
    'working_directory',    # Where to start the CU
    'input',                # stdin
    'error',                # stderr
    'output',               # stdout
    'file_transfer',        # file transfer, duh!
    'input_data',           # DataUnits for input.
    'output_data',          # DataUnits for output.

    # Parallelism
    'spmd_variation',       # Type and startup mechanism.
    'slots',                # Number of job slots for spmd variations that
                            # support it.

    # resource requirements
    'cpu_architecture',     # Specific requirement for binary
    'operating_system_type',# Specific OS version required?
    'total_physical_memory',# May not be physical, but in sync with saga.

    # Startup ordering dependencies
    # (Are only considered within scope of bulk submission.)
    'run_after',            # Names of CUs that need to finish first.
    'start_after',          # Names of CUs that need to start  first.
    'start_concurrent_with' # Names of CUs that need to run concurrently.
    """
    def __init__ (self, vals={}) : 

        sa.Description.__init__ (self, vals)

        # register properties with the attribute interface
        # action description
        self._attributes_register  (NAME,              None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (EXECUTABLE,        None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (ARGUMENTS,         None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (ENVIRONMENT,       None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (CLEANUP,           None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (START_TIME,        None, sa.TIME,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (RUN_TIME,          None, sa.TIME,   sa.SCALAR, sa.WRITEABLE)

        # I/O
        self._attributes_register  (WORKING_DIRECTORY, None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (INPUT,             None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (OUTPUT,            None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (ERROR,             None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (FILE_TRANSFER,     None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (INPUT_DATA,        None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (OUTPUT_DATA,       None, sa.STRING, sa.VECTOR, sa.WRITEABLE)

        # parallelism
        self._attributes_register  (SPMD_VARIATION,    None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (SLOTS,             None, sa.INT,    sa.SCALAR, sa.WRITEABLE)

        # resource requirements
        self._attributes_register  (CPU_ARCHITECTURE,  None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (OPERATING_SYSTEM,  None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (MEMORY,            None, sa.INT,    sa.SCALAR, sa.WRITEABLE)

        # dependencies
        self._attributes_register  (RUN_AFTER,         None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (START_AFTER,       None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (CONCURRENT_WITH,   None, sa.STRING, sa.VECTOR, sa.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

