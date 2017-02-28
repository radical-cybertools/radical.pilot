
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import time

import radical.utils as ru

from ..utils import Worker
from .. import constants as rpc



# ==============================================================================
# defaults
DEFAULT_HEARTBEAT_INTERVAL = 10.0   # seconds


# ==============================================================================
#
class Heartbeat(Worker):
    """
    The Heartbeat worker watches the command queue for heartbeat updates (and
    other commands).
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id('heartbeat.%(counter)s', ru.ID_CUSTOM)

        Worker.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session):

        return cls(cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):

        self._owner         = self._cfg['owner']
        self._dburl         = self._cfg['dburl']
        self._runtime       = self._cfg.get('runtime')
        self._starttime     = time.time()


# ------------------------------------------------------------------------------

