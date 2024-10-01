
__copyright__ = 'Copyright 2022-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'


import threading as mt
import collections

from ... import states as rps

from .base import AgentResolvingComponent


# ------------------------------------------------------------------------------
#
class EnvPrep(AgentResolvingComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        super().__init__(cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        super().initialize()

        # keep track of known named envs
        self._named_envs = list()
        self._waitpool   = collections.defaultdict(list)
        self._lock       = mt.Lock()


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):
        '''
        listen on the control channel for raptor queue registration commands
        '''

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'register_named_env':

            with self._lock:
                env_name = arg['env_name']
                self._named_envs.append(env_name)

                # if we have tasks waiting for this env, now is the time to push
                # them out
                if env_name in self._waitpool:
                    self.advance(self._waitpool[env_name],
                                 rps.AGENT_STAGING_INPUT_PENDING,
                                 publish=True, push=True)
                    del self._waitpool[env_name]

        return True


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_RESOLVING, publish=True, push=False)

        to_advance = list()
        with self._lock:

            for task in tasks:

                env_name = task['description'].get('named_env')

                if not env_name or env_name in self._named_envs:
                    to_advance.append(task)

                else:
                    self._log.debug('delay %s, no env %s', task['uid'], env_name)
                    self._waitpool[env_name].append(task)

        if to_advance:
            self.advance(to_advance, rps.AGENT_STAGING_INPUT_PENDING,
                         publish=True, push=True)


# ------------------------------------------------------------------------------

