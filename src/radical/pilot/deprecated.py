
import sys


# ------------------------------------------------------------------------------
#
# make sure deprecation warning is shown only once per type
#
_seen = list()

def _warn(old_type, new_type):
    if old_type not in _seen:
        _seen.append(old_type)
        sys.stderr.write('%s is deprecated - use %s\n' % (old_type, new_type))


# ------------------------------------------------------------------------------
#
from .task_description import TaskDescription

class ComputeUnitDescription(TaskDescription):

    def __init__(self, *args, **kwargs):

        _warn(ComputeUnitDescription, TaskDescription)
        TaskDescription.__init__(self, *args, **kwargs)


# ------------------------------------------------------------------------------
#
from .task import Task

class ComputeUnit(Task):

    def __init__(self, *args, **kwargs):

        _warn(ComputeUnit, Task)
        Task.__init__(self, *args, **kwargs)


# ------------------------------------------------------------------------------
#
from .pilot_description import PilotDescription

class ComputePilotDescription(TaskDescription):

    def __init__(self, *args, **kwargs):

        _warn(ComputePilotDescription, PilotDescription)
        PilotDescription.__init__(self, *args, **kwargs)


# ------------------------------------------------------------------------------
#
from .pilot import Pilot

class ComputePilot(Pilot):

    def __init__(self, *args, **kwargs):

        _warn(ComputePilot, Pilot)
        Pilot.__init__(self, *args, **kwargs)


# ------------------------------------------------------------------------------
#
from .task_manager import TaskManager

class UnitManager(TaskManager):

    def __init__(self, *args, **kwargs):

        _warn(UnitManager, TaskManager)
        TaskManager.__init__(self, *args, **kwargs)


# ------------------------------------------------------------------------------

