
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__ = "MIT"


import os
import inspect
import logging
import pprint


import threading     as mt

import radical.utils as ru

from ...   import constants as rpc
from .base import AgentSchedulingComponent

import logging  # delayed import for atfork


# ------------------------------------------------------------------------------
#
# This is a scheduler which does not schedule, at all.  It leaves all placement
# to executors such as srun, jsrun, aprun etc.
#
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
import cProfile
cprof = cProfile.Profile()


def cprof_it(func):
    def wrapper(*args, **kwargs):
        retval = cprof.runcall(func, *args, **kwargs)
        return retval
    return wrapper


def dec_all_methods(dec):
    def dectheclass(cls):
        if ru.is_main_thread():
            cprof_env   = os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "")
            cprof_elems = cprof_env.split()
            if "CONTINUOUS" in cprof_elems:
                for name, m in inspect.getmembers(cls, inspect.ismethod):
                    setattr(cls, name, dec(m))
        return cls
    return dectheclass


# ------------------------------------------------------------------------------
#
@dec_all_methods(cprof_it)
class Noop(AgentSchedulingComponent):
    '''
    The Noop scheduler does not perform any placement.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # FIXME: this should not be overloaded here, but in the base class
    #
    def finalize_child(self):

        cprof_env = os.getenv("RADICAL_PILOT_CPROFILE_COMPONENTS", "")
        if "NOOP" in cprof_env.split():
            self_thread = mt.current_thread()
            cprof.dump_stats("python-%s.profile" % self_thread.name)

        # make sure that parent finalizers are called
        super(Noop, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        pass


    # --------------------------------------------------------------------------
    #
    def _try_allocation(self, unit):

        # signal success
        return True


# ------------------------------------------------------------------------------

