

from   description import Description
from   attributes  import *
from   constants   import *


# ------------------------------------------------------------------------------
#
class DataUnitDescription (Description) :
    """
    name         # A non-unique label.
    file_urls    # Dict of logical and physical filesnames, e.g.:
                    # { 'NAME1' : [ 'google://.../name1.txt',
                    #               'srm://grid/name1.txt'],
                    #   'NAME2' : [ 'file://.../name2.txt' ] }
    lifetime     # Needs to stay available for at least ...
    cleanup      # Can be removed when cancelled
    size         # Estimated size of DU (in bytes)
    """
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals={}) : 

        Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (NAME,              None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (FILE_URLS,         None, DICT,   SCALAR, WRITEABLE)
        self._attributes_register  (LIFETIME,          None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (CLEANUP,           None, BOOL,   SCALAR, WRITEABLE)
        self._attributes_register  (SIZE,              None, INT,    SCALAR, WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

