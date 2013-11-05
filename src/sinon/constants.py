"""
.. module:: sinon.constants
   :platform: Unix
   :synopsis: Definition of SAGA-Pilot-wide constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

# ------------------------------------------------------------------------------
# Unit and Pilot types
DATA              = 1
COMPUTE           = 2
ANY               = DATA | COMPUTE

# ------------------------------------------------------------------------------
# States
UNKNOWN           = 'Unknown'
PENDING           = 'Pending'
RUNNING           = 'Running'
ACTIVE            =  RUNNING
DONE              = 'Done'
CANCELED          = 'Canceled'
FAILED            = 'Failed' 

# ------------------------------------------------------------------------------
# State Metrics
STATE             = 'State'
STATE_DETAIL      = 'StateDetail'

# ------------------------------------------------------------------------------
# Inspection Metrics
SUBMIT_TIME       = 'SubmitTime'
START_TIME        = 'StartTime'
END_TIME          = 'EndTime'

DESCRIPTION       = 'Description'
SCHEDULER         = 'Scheduler'

UID               = 'UID'
UNITS             = 'Units'
UNIT_MANAGER      = 'UnitManager'
UNIT_MANAGERS     = 'UnitManagers'

PID               = 'PID'
PILOT             = 'Pilot'
PILOTS            = 'Pilots'
PILOT_MANAGER     = 'PilotManager'

# ------------------------------------------------------------------------------
# ComputeUnitDescription keys
RESOURCE          = 'Resource'
NAME              = 'Name'
EXECUTABLE        = 'Executable'
ARGUMENTS         = 'Arguments'
ENVIRONMENT       = 'Environment'
ALLOCATION        = 'Allocation'
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
CORES             = 'Cores'

CPU_ARCHITECTURE  = 'Cpu_architecture'
OPERATING_SYSTEM  = 'OperatingSystem'
MEMORY            = 'Memory'

RUN_AFTER         = 'RunAfter'
RUN_BEFORE        = 'RunBefore'
START_AFTER       = 'StartAfter'
START_BEFORE      = 'StartBefore'
CONCURRENT_WITH   = 'ConcurrentWith'

# ------------------------------------------------------------------------------
# DataUnitDescription keys
NAME              = 'Name'
FILE_URLS         = 'FileURLs'
LIFETIME          = 'Lifetime'
CLEANUP           = 'Cleanup'
SIZE              = 'Size'

# ------------------------------------------------------------------------------
# ComputePilotDescription keys
RESOURCE          = 'Resource'
