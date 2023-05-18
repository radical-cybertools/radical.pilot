

from attributes import *
from constants  import *


# ------------------------------------------------------------------------------
#
class TaskManager (Attributes) :
    """
    TaskManager class -- manages a pool
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self, url=None, scheduler='default', session=None) :

        Attributes.__init__ (self)


    # --------------------------------------------------------------------------
    #
    def add_pilot (self, pid) :
        """
        add Pilot(s) to the pool
        """

        raise Exception ("%s.add_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def list_pilots (self, ptype=ANY) :
        """
        List IDs of data and/or pilots
        """

        raise Exception ("%s.list_pilots() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def remove_pilot (self, pid, drain=False) :
        """
        Remove pilot(s) (does not cancel the pilot(s), but removes all tasks
        from the pilot(s).

        `drain` determines what happens to the tasks which are managed by the
        removed pilot(s).  If `True`, the pilot removal is delayed until all
        tasks reach a final state.  If `False` (the default), then `RUNNING`
        tasks will be canceled, and `PENDING` tasks will be re-assinged to the
        task managers for re-scheduling to other pilots.
        """

        raise Exception ("%s.remove_pilot() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def submit_task (self, description) :
        """
        Instantiate and return Task object(s)
        """

        raise Exception ("%s.submit_task() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def list_tasks (self, utype=ANY) :
        """
        List IDs of data and/or tasks
        """

        raise Exception ("%s.list_tasks() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def get_task (self, uids) :
        """
        Reconnect to and return Task object(s)
        """

        raise Exception ("%s.get_task() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def wait_task (self, uids, state=[DONE, FAILED, CANCELED], timeout=-1.0) :
        """
        Wait for given task(s) to enter given state
        """

        raise Exception ("%s.wait_task() is not implemented" % self.__class__.__name__)


    # --------------------------------------------------------------------------
    #
    def cancel_tasks (self, uids) :
        """
        Cancel given task(s)
        """

        raise Exception ("%s.cancel_task() is not implemented" % self.__class__.__name__)


# ------------------------------------------------------------------------------
#


