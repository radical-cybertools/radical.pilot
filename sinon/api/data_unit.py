

from unit      import Unit
from constants import *


# ------------------------------------------------------------------------------
#
class DataUnit (Unit) :
    """ 
    DataUnit class.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, uid) : 

        Unit.__init__ (self, uid)


    # --------------------------------------------------------------------------
    #
    def import_data (self, src, ttype=SYNC) :
        """
        For a data unit which does not point to PFNs yet, create a first PFN as
        copy from the given src URL.

        FIXME: what happens if we already have PFNs?
        """

        raise Exception ("%s.import_data() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def export_data (self, tgt, ttype=SYNC) :
        """
        Copy any of the data_unit's PFNs to the tgt URL.
        """

        raise Exception ("%s.export_data() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def remove_data (self, ttype=SYNC) :
        """
        Removes the data.  Implies cancel ()
        """

        raise Exception ("%s.remove_data() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

