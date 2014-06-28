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

# ------------------------------------------------------------------------------
#
class X509Credential(object):
    """An X509Credential object represents an X509 identity.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :
        """Creates a new X509Credential object.
        """
        self._context = sc.Context("X509")

    #---------------------------------------------------------------------------
    #
    @classmethod
    def from_dict(cls, thedict):
        """Creates a new object instance from a string.

                c._from_dict(x.as_dict) == x
        """
        obj = cls()
        obj._context.USER_PROXY = thedict["user_proxy"]
        obj._context.LIFE_TIME  = thedict["life_time"]

        return obj

    # --------------------------------------------------------------------------
    #
    def as_dict(self):
        """Returns a Python dictionary representation of the object.
        """
        dct = {
            "type"       : "SSH",
            "user_proxy" : self._context.user_proxy,
            "life_time"  : self._context.life_time,
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
    def user_proxy(self):
        """ TODO: Document me
        """
        return self._context.user_proxy
    @user_proxy.setter
    def user_proxy(self, value):
        """ TODO: document me
        """
        self._context.user_proxy = value

    # --------------------------------------------------------------------------
    #
    @property
    def life_time(self):
        """ TODO: Document me
        """
        return self._context.life_time
    @life_time.setter
    def life_time(self, value):
        """ TODO: Document me
        """
        self._context.life_time = value
