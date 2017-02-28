

from description import Description


# ------------------------------------------------------------------------------
#
class DataUnitDescription(Description):
    """

    A DataUnitDescription (DUD) contains all references to the input files
    that should be used to initially populate the DataUnit.

    Having submitted the description object, a ComputeUnit/DataUnit ID is
    returned.

    This ID can then be used for state queries and lifecycle management
    (e.g. canceling a CU).

    In case (ii), the runtime system of the Compute-Data Service is responsible
    for placing CUs and DUs on a Pilot.
    For this purpose, it relies on different information and heuristics e.g.
    on the localities of the DUs, to facilitate scheduling and other types of
    decision making (see section 4.5)

    name         # A non-unique label.
    files        # Dict of logical and physical filesnames, e.g.:
                    # { 'NAME1' : [ 'google://.../name1.txt',
                    #               'srm://grid/name1.txt'],
                    #   'NAME2' : [ 'file://.../name2.txt' ] }
    lifetime     # Needs to stay available for at least ...
    cleanup      # Can be removed when cancelled
    size         # Estimated size of DU (in bytes)
    """
    
    # --------------------------------------------------------------------------
    #
    def __init__(self, vals={}):

        Description.__init__ (self, vals)


# ------------------------------------------------------------------------------
#
