"""
.. module:: sinon.description
   :platform: Unix
   :synopsis: Implementation of the Description base class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, RADICAL Group at Rutgers University"
__license__   = "MIT"

import attributes
import constants

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

        self._attributes_register    ('dtype',  constants.UNKNOWN, attributes.ENUM, attributes.SCALAR, attributes.READONLY)
        self._attributes_set_enums   ('dtype', [constants.UNKNOWN, constants.COMPUTE, constants.DATA])
