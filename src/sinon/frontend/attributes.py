"""
.. module:: sinon.attributes
   :platform: Unix
   :synopsis: Implementation of the attribute interface and constants.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

import saga.attributes  as satt

# ------------------------------------------------------------------------------
#
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


# ------------------------------------------------------------------------------
#
class Attributes (satt.Attributes) :

    def __init__ (self, *args, **kwargs) :

        satt.Attributes.__init__ (self, *args, **kwargs)

    def __str__ (self) :
        return str (self.as_dict ())
