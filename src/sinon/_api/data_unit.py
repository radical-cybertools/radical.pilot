

from unit import Unit
from constants import *


# ------------------------------------------------------------------------------
#
class DataUnit(Unit):
    """ 
    A DataUnit represents a self-contained, related set of data.
    A DataUnit is defined as an immutable container for a logical group of
    "affine" data files, e. g. data that is often accessed together
    e.g. by multiple ComputeUnits.

    This simplifies distributed data management tasks, such as data placement,
    replication and/or partitioning of data, abstracting low-level details,
    such as storage service specific access detail.

    A DU is completely decoupled from its physical location and can be
    stored in different kinds of backends, e. g. on a parallel filesystem,
    cloud storage or in-memory.

    Replicas of a DU can reside in different DataPilots.

    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, uid):

        Unit.__init__(self, uid)

    # --------------------------------------------------------------------------
    #
    def import_data(self, src):
        """
        For a data unit which does not point to PFNs yet, create a first PFN as
        copy from the given src URL.

        FIXME: what happens if we already have PFNs?
        """

        raise Exception("%s.import_data() is not implemented" % self.__class__.__name__)

    # --------------------------------------------------------------------------
    #
    def export_data(self, tgt):
        """
        Copy any of the data_unit's PFNs to the tgt URL.
        """

        raise Exception ("%s.export_data() is not implemented" % self.__class__.__name__)

    # --------------------------------------------------------------------------
    #
    def remove_data(self):
        """
        Removes the data.  Implies cancel ()
        """

        raise Exception("%s.remove_data() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#

