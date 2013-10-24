

from description import Description


# ------------------------------------------------------------------------------
#

class ComputePilotDescription (Description) :
    """
    The ComputePilotDescription is a description based on
    SAGA Job Description.

    It offers the application to describe a ComputePilot in an abstract
    way that is dealt with by the Pilot-Manager.

    Class members:

        # Action description
        'start_time',               # pilot is not needed before X
        'run_time',                 # pilot is not needed after  X
        'cleanup',
        'project',

        # I/O
        # reconsider for SP2
        'working_directory',
        'error',                    # stderr
        'output',                   # stdout
        'file_transfer',            # out/err staging

        # Parallelism 
        # reconsider for SP2
        'slots'                     # total number of cores
        'spmd_variation',           # expected startup mechanism for CUs (def. None)

        # resource requirements
        'candidate_hosts',          # List of specific hostnames to run on.
        'cpu_architecture',         # Specify specific arch required.
        'total_physical_memory',    # mem for CU usage
        'operating_system_type',    # Specify specific OS required.
        'queue'                     # Specify queue name of backend system.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)


# ------------------------------------------------------------------------------
#


