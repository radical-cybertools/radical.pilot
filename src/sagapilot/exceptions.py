#pylint: disable=C0301, C0103, W0212

"""
.. module:: sagapilot.exceptions
   :platform: Unix
   :synopsis: Implementation of the exception classes.

.. moduleauthor:: Ole Weidner <ole.weidner@rutgers.edu>
"""

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
class SagapilotException(Exception):
    """
    :param msg: Error message, indicating the cause for the exception
                being raised.
    :type  msg: string
    :param obj: SAGA-Pilot object on whose activity the exception was raised.
    :type  obj: object
    :raises:    --

    The base class for all  SAGA-Pilot Exception classes -- this exception type is
    never raised directly, but can be used to catch all SAGA-Pilot exceptions within
    a single `except` clause.

    The exception message and originating object are also accessable as class
    attributes (:func:`e.object` and :func:`e.message`).  The :func:`__str__`
    operator redirects to :func:`get_message()`.  
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, msg, obj=None) :
        """Le constructeur. Creates a new exception object. 
        """
        Exception.__init__(self, msg)
        self._obj = obj
        self.message = msg

    # --------------------------------------------------------------------------
    #
    def get_object (self) :
        """
        Return the object instance on whose activity the exception was raised.
        """
        return self._obj


    # --------------------------------------------------------------------------
    #
    def get_message (self) :
        """
        Return the error message associated with the exception
        """
        return self.message


    # --------------------------------------------------------------------------
    #
    def __str__ (self) :
        """
        an alias for `get_message()`
        """
        return self.get_message()


# ------------------------------------------------------------------------------
#
class DatabaseError (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)

# ------------------------------------------------------------------------------
#
class IncorrectURL (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class BadParameter (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class DoesNotExist (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class IncorrectState (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class PermissionDenied (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class AuthorizationFailed (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class AuthenticationFailed (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class Timeout (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)


# ------------------------------------------------------------------------------
#
class NoSuccess (SagapilotException) :
    """
    TODO: Document me!
    """
    def __init__ (self, msg, obj=None) :
        SagapilotException.__init__(self, msg, obj)
