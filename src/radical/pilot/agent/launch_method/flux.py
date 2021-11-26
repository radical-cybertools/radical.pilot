
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils   as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, session, prof):

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, session, prof)


    # --------------------------------------------------------------------------
    #
    def _terminate(self):

        if self._fh:
            self._fh.reset()


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        self._prof.prof('flux_start')
        self._fh = ru.FluxHelper()

      # self._fh.start_flux(env=env)  # FIXME

        self._log.debug('starting flux')
        self._fh.start_flux()

        self._details = {'flux_uri': self._fh.uri,
                         'flux_env': self._fh.env}

        self._prof.prof('flux_start_ok')

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'details': self._details}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._prof.prof('flux_reconnect')

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._details = lm_info['details']

        self._fh = ru.FluxHelper()
        self._fh.connect_flux(uri=self._details['flux_uri'])

        self._prof.prof('flux_reconnect_ok')


    # --------------------------------------------------------------------------
    #
    @property
    def fh(self):
        return self._fh


    def can_launch(self, task):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launch_cmds(self, task, exec_path):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launcher_env(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_cmd(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_exec(self, task, rank_id, rank):
        raise RuntimeError('method cannot be used on Flux LM')


# ------------------------------------------------------------------------------

