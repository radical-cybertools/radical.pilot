#pylint: disable=C0301, C0103, W0212, E1101, R0903

"""
.. module:: sagapilot.attributes
   :platform: Unix
   :synopsis: Implementation of the attribute interface and constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes  as satt

# ------------------------------------------------------------------------------
# Attribute types
ANY         = satt.ANY
URL         = satt.URL
INT         = satt.INT
FLOAT       = satt.FLOAT
STRING      = satt.STRING
BOOL        = satt.BOOL
ENUM        = satt.ENUM
TIME        = satt.TIME

WRITEABLE   = satt.WRITEABLE
READONLY    = satt.READONLY
FINAL       = satt.FINAL
ALIAS       = satt.ALIAS

EXTENDED    = satt.EXTENDED
PRIVATE     = satt.PRIVATE

SCALAR      = satt.SCALAR
DICT        = satt.DICT
VECTOR      = satt.VECTOR

#------------------------------------------------------------------------------
#
class Attributes(satt.Attributes):
    """Attributes provides an attribute interface for ComputePilotDescription
    and ComputeUnitDescription. It's really just a forward declaration of 
    saga.Attributes.
    """

    #---------------------------------------------------------------------------
    #
    def __init__(self, *args, **kwargs) :
        """Le constructeur.
        """
        satt.Attributes.__init__ (self, *args, **kwargs)

    #---------------------------------------------------------------------------
    #
    def __str__ (self) :
        """Returns a string representation of the object.
        """
        return str (self.as_dict())
