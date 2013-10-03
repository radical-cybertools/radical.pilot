
# Async call types
SYNC              = 'Sync';         """The call will be synchronous."""
ASYNC             = 'Async'

# Unit and Pilot types
DATA              = 1
COMPUTE           = 2
ANY               = DATA | COMPUTE

# States
UNKNOWN           =  0
PENDING           =  1
ACTIVE            =  2
DONE              =  4
CANCELED          =  8
FAILED            = 16
FINAL             = DONE | CANCELED | FAILED

# State Metrics
STATE             = 'State'
STATE_DETAIL      = 'StateDetail'

# Inspection Metrics
SUBMIT_TIME       = 'SubmitTime'
START_TIME        = 'StartTime'
END_TIME          = 'EndTime'

DESCRIPTION       = 'Description'
SCHEDULER         = 'Scheduler'

UID               = 'PID'
UNITS             = 'Units'
UNIT_MANAGER      = 'UnitManager'
UNIT_MANAGERS     = 'UnitManagers'

PID               = 'PID'
PILOT             = 'Pilot'
PILOTS            = 'Pilots'
PILOT_MANAGER     = 'PilotManager'
# ...

# ComputeUnitDescription keys
NAME              = 'Name'
EXECUTABLE        = 'Executable'
ARGUMENTS         = 'Arguments'
ENVIRONMENT       = 'Environment'
PROJECT           = 'Project'
QUEUE             = 'Queue'
CANDIDATE_HOSTS   = 'CandidateHosts'
CLEANUP           = 'Cleanup'
START_TIME        = 'StartTime'
RUN_TIME          = 'RunTime'

WORKING_DIRECTORY = 'WorkingDirectory'
INPUT             = 'Input'
OUTPUT            = 'Output'
ERROR             = 'Error'
FILE_TRANSFER     = 'FileTransfer'
INPUT_DATA        = 'InputData'
OUTPUT_DATA       = 'OutputData'

SPMD_VARIATION    = 'SPMDVariation'
SLOTS             = 'Slots'

CPU_ARCHITECTURE  = 'Cpu_architecture'
OPERATING_SYSTEM  = 'OperatingSystem'
MEMORY            = 'Memory'

RUN_AFTER         = 'RunAfter'
RUN_BEFORE        = 'RunBefore'
START_AFTER       = 'StartAfter'
START_BEFORE      = 'StartBefore'
CONCURRENT_WITH   = 'ConcurrentWith'

# DataUnitDescription keys
# NAME            = 'Name'       # duplicate
FILE_URLS         = 'FileURLs'
LIFETIME          = 'Lifetime'
CLEANUP           = 'Cleanup'
SIZE              = 'Size'


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

