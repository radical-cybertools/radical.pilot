
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
        self._mongodb_url   = self._cfg['mongodb_url']
        self._runtime       = self._cfg.get('runtime')
        self._starttime     = time.time()

        # register work routine (this component is lazy, and only registers an
        # idle callback)
        self.declare_idle_cb(self.idle_cb, timeout=self._cfg.get('heartbeat_interval',
                                                                 DEFAULT_HEARTBEAT_INTERVAL))


    # --------------------------------------------------------------------------
    #
    def idle_cb(self):

        try:
            self._prof.prof('heartbeat', msg='Listen! Listen! Listen to the heartbeat!',
                            uid=self._owner)
            self._check_commands()
            self._check_state   ()
            return True

        except Exception as e:
            self._log.exception('heartbeat died - cancel')
            self.publish('control', {'cmd' : 'shutdown',
                                     'arg' : 'exception'})

    # --------------------------------------------------------------------------
    #
    def _check_commands(self):

        # Check if there's a command waiting
        # FIXME: this pull should be done by the update worker, and commands
        #        should then be communicated over the command pubsub
        # FIXME: commands go to pmgr, umgr, session docs
        # FIXME: this is disabled right now
        return
        retdoc = self._session._dbs._c.find_and_modify(
                    query  = {"uid"  : self._owner},
                    update = {"$set" : {rpc.COMMAND_FIELD: []}}, # Wipe content of array
                    fields = [rpc.COMMAND_FIELD]
                    )

        if not retdoc:
            return

        import pprint
        self._log.debug('hb %s got %s: %s (%s)', 
                self._owner, type(retdoc), pprint.pformat(retdoc),
                str(retdoc['_id']))

        for command in retdoc.get(rpc.COMMAND_FIELD, []):

            cmd = command[rpc.COMMAND_TYPE]
            arg = command[rpc.COMMAND_ARG]

            self._prof.prof('ingest_cmd', msg="mongodb to HeartbeatMonitor (%s : %s)" \
                            % (cmd, arg), uid=self._owner)

            if cmd == rpc.COMMAND_CANCEL_PILOT:
                self._log.info('cancel pilot cmd')
                self.publish('control', {'cmd' : 'shutdown',
                                         'arg' : 'cancel'})

            elif cmd == rpc.COMMAND_CANCEL_COMPUTE_UNIT:
                self._log.info('cancel unit cmd')
                self.publish('control', {'cmd' : 'cancel_unit',
                                         'arg' : command})

            elif cmd == rpc.COMMAND_KEEP_ALIVE:
                self._log.info('keepalive pilot cmd')
                self.publish('control', {'cmd' : 'heartbeat',
                                         'arg' : 'keepalive'})


    # --------------------------------------------------------------------------
    #
    def _check_state(self):

        # Make sure that we haven't exceeded the runtime (if one is set). If
        # we have, terminate.
        if self._runtime:
            if time.time() >= self._starttime + (int(self._runtime) * 60):
                self._log.info("reached runtime limit (%ss).", self._runtime*60)
                self.publish('control', {'cmd' : 'shutdown',
                                         'arg' : 'timeout'})


# ------------------------------------------------------------------------------

