"""
.. module:: sinon.exceptions
   :platform: Unix
   :synopsis: Implementation of the Exception class(es).

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, radical.rutgers.edu"
__license__   = "MIT"

import saga.exceptions as se

# ------------------------------------------------------------------------------
#
class SinonException (se.SagaException) :

    def __init__ (self, msg, obj=None) :

        se.SagaException.__init__ (self, msg, obj)

