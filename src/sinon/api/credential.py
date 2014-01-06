"""
.. module:: sinon.context
   :platform: Unix
   :synopsis: Implementation of the Context class.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.context as sc

# ------------------------------------------------------------------------------
#
class Credential(sc.Context):
    """A Credential object represent a security credential, like for example
    an SSH identity.
    """ 

    # ------------------------------------------------------------------------------
    #
    def __init__ (self, ctype) :
        """Creates a new Credential object.
        """

        sc.Context.__init__ (self, ctype)
