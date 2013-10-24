

from attributes import *


# ------------------------------------------------------------------------------
#
class Description (Attributes) :
    """ 
    Base class for ComputeUnitDescription, DataUnitDescription,
    ComputePilotDescription and DataPilotDescription.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals=None) :
        """
        Base class for the different description classes of the Pilot API.  Any
        description can be initialized from a dictionary -- but that
        initialization will fail on unsupported dictionary keys or value types.
    
        :param vals:  dictionary to initialize attributes
        :type  vals:  dictionary or None
        :returns   :  an instance of the Description base class
        :rtype     :  Description
        :raises    :  BadParameter (on invalid initialization)
        """

        Attributes.__init__ (self, vals)


# ------------------------------------------------------------------------------
#


