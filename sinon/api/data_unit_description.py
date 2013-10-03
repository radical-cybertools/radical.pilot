

from   description import Description

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


# ------------------------------------------------------------------------------
#
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

