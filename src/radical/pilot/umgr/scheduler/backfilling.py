
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from ... import utils     as rpu
from ... import states    as rps
from ... import constants as rpc

from .base import UMGRSchedulingComponent


# ==============================================================================
#
class Backfilling(UMGRSchedulingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.pilots = None

        UMGRSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.pilots = None   # FIXME: fill
        self.idx    = 0


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cores_requested):

        pids = self.pilots.keys ()

        if not len(pids):
            raise RuntimeError ('Unit scheduler cannot operate on empty pilot set')


        if  self.idx >= len(pids) : 
            self.idx = 0
        
        return pids[self.idx]


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, opaque_slots):

        pass


# ------------------------------------------------------------------------------

