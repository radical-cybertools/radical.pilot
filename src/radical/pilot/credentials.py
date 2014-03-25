#pylint: disable=C0301, C0103, W0212

"""
.. module:: radical.pilot.context
   :platform: Unix
   :synopsis: Implementation of the Context class(es).

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import saga.context as sc

# ------------------------------------------------------------------------------
#
class SSHCredential(object):
    """An SSHCredential object represents an SSH identity.
    """ 

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :
        """Creates a new SSHCredential object.
        """
        self._context = sc.Context("SSH")

    #---------------------------------------------------------------------------
    #
    @classmethod
    def from_dict(cls, thedict):
        """Creates a new object instance from a string.

                c._from_dict(x.as_dict) == x
        """
        obj = cls()
        obj._context.user_id   = thedict["user_id"]
        obj._context.user_pass = thedict["user_pass"]
        obj._context.user_key  = thedict["user_key"]

        return obj

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        dct = {
            "type"      : "SSH",
            "user_id"   : self._context.user_id,
            "user_pass" : self._context.user_pass,
            "user_key"  : self._context.user_key,
        }
        return dct

    # --------------------------------------------------------------------------
    #
    def __str__ (self):
        """Returns the string representation of the object.
        """
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    @property
    def user_id(self):
        """ TODO: Document me 
        """
        return self._context.user_id
    @user_id.setter
    def user_id(self, value):
        """ TODO: document me
        """
        self._context.user_id = value
    
    # --------------------------------------------------------------------------
    #
    @property
    def user_pass(self):
        """ TODO: Document me 
        """
        return self._context.user_pass
    @user_pass.setter
    def user_pass(self, value):
        """ TODO: Document me 
        """
        self._context.user_pass = value

    # --------------------------------------------------------------------------
    #
    @property
    def user_key(self):
        """ TODO: Document me 
        """
        return self._context.user_key
    @user_key.setter
    def user_key(self, value):
        """ TODO: Document me 
        """
        self._context.user_key = value
