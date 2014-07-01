#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.object
   :platform: Unix
   :synopsis: Implementation of a base object.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

from exceptions import IncorrectState

class Object(object):

    # AM: this seems to be only used in session and pilot_manager -- is there
    # a reason why this is not used for all 'Objects' which have a uid and can
    # be closed?


    #---------------------------------------------------------------------------
    #
    def __init__(self):
        self._uid = None

    #---------------------------------------------------------------------------
    #
    def _assert_obj_is_valid(self):
        if not self._uid:
            msg = "Invalid session instance: closed or doesn't exist."
            raise IncorrectState(msg=msg)

    #---------------------------------------------------------------------------
    #
    @property
    def uid(self):
        """Returns the object's unique identifier.

        **Returns:**
            * A unique identifier (`string`).

        **Raises:**fg
            * :class:`radical.pilot.IncorrectState` if the session is closed
              or doesn't exist. 

        """
        self._assert_obj_is_valid()
        return self._uid
