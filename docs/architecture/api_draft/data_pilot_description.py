

from description import Description


# ------------------------------------------------------------------------------
#

class DataPilotDescription(Description):
    """
    resource_url        # The URL of the service endpoint
    size                # Storage size of DP (in bytes)

    # AM: lifetime, resource information, etc. ?
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, vals={}):

        Description.__init__(self, vals)


# ------------------------------------------------------------------------------
#
