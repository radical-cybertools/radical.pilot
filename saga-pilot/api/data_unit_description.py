

from description import Description


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

        sa.Description.__init__ (self, vals)

        # register properties with the attribute interface
        self._attributes_register  (NAME,              None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (FILE_URLS,         None, sa.DICT,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (LIFETIME,          None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CLEANUP,           None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (SIZE,              None, sa.INT,    sa.SCALAR, sa.WRITEABLE)


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

