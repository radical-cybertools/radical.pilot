

from   description import Description


# ------------------------------------------------------------------------------
#
class TaskDescription (Description) :
    """
    # Action description
    'name',                 # Non-unique name/label of Task.
    'executable',           # The "action" to execute
    'arguments',            # Arguments to the "action"
    'environment',          # "environment" settings for the "action"
    'cleanup',              # cleanup after the Task has finished
    'start_time',           # When should the Task start
    'run_time',             # When is the Task expected to finish

    # I/O
    'working_directory',    # Where to start the Task
    'input',                # stdin
    'error',                # stderr
    'output',               # stdout
    'file_transfer',        # file transfer, duh!
    'input_data',           # DataTasks for input.
    'output_data',          # DataTasks for output.

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

    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)


# ------------------------------------------------------------------------------
#


