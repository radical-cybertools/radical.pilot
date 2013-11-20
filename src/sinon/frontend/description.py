"""
.. module:: sinon.description
   :platform: Unix
   :synopsis: Implementation of the Description base class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

from sinon.constants import * 

import sinon.frontend.types as types
import sinon.frontend.attributes as attributes

# ------------------------------------------------------------------------------
#
class Description (attributes.Attributes) :
    
    # --------------------------------------------------------------------------
    #
    def __init__ (self, vals=None) :

        attributes.Attributes.__init__ (self, vals)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        self._attributes_register    ('dtype',  types.UNKNOWN, attributes.ENUM, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_enums   ('dtype', [types.UNKNOWN, types.COMPUTE, types.DATA])
