"""
.. module:: sinon.context
   :platform: Unix
   :synopsis: Implementation of the Context class(es).

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013, http://radical.rutgers.edu"
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

    # --------------------------------------------------------------------------
    #
    def __str__ (self):
        """Returns the string representation of the credential.
        """
        return str(self.as_dict())

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns the dictionary representation of the credential. 
        """
        dct = {
            "Type"      : "SSH",
            "UserID"    : self._context.user_id,
            "UserPass"  : self._context.user_pass,
            "UserKey"   : self._context.user_key,
        }
        return dct

    # --------------------------------------------------------------------------
    #
    @property
    def user_id(self):
        """ XXX
        """
        return self._context.user_id
    @user_id.setter
    def user_id(self, value):
        """
        """
        self._context.user_id = value
    
    # --------------------------------------------------------------------------
    #
    @property
    def user_pass(self):
        """ XXX
        """
        return self._context.user_pass
    @user_pass.setter
    def user_pass(self, value):
        """ XXX
        """
        self._context.user_pass = value

    # --------------------------------------------------------------------------
    #
    @property
    def user_key(self):
        """ XXX
        """
        return self._context.user_key
    @user_key.setter
    def user_key(self, value):
        """ XXX
        """
        self._context.user_key = value
