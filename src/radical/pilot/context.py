"""Implementation of the Context class(es).

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.saga as rs


# ------------------------------------------------------------------------------
#
class Context (rs.Context):

    # --------------------------------------------------------------------------
    #
    def __init__ (self, ctype, from_dict=None) :

        # init the saga.Context
        self._apitype  = 'radical.saga.Context'
        super (Context, self).__init__(ctype)

        # set given defaults
        if from_dict:
            for key in from_dict:
                self.set_attribute (key, from_dict[key])


# ------------------------------------------------------------------------------
