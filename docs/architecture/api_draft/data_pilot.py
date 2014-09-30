

from pilot import Pilot
from constants import *


# ------------------------------------------------------------------------------
#
class DataPilot(Pilot):
    """ 
    A DataPilot represents a physical storage resource that is used as a
    logical container for dynamic data placement.

    E.g. for compute-local data replicas or for caching intermediate data.

    A DataPilot refers to a physical storage location, e. g. a directory on
    a local or remote filesystem or a bucket in a cloud storage service.

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, pid):

        Pilot.__init__(self, pid)


# ------------------------------------------------------------------------------
#

