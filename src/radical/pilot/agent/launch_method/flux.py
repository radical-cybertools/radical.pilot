
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
    def _configure(self, env):

        self._helper = ru.FluxHelper()
        self._finfo  = self._helper.start_service(env=env)
        self._fuid   = self._finfo['uid']
        self._fex    = self._helper.get_executor(self._fuid)
        self._fh     = self._helper.get_handle(self._fuid)

        self._prof.prof('flux_start')

        self._details = self._fh.get_info()

        return self._details


    # --------------------------------------------------------------------------
    #
    def _terminate(self):

        self._fh.close_service()


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'details': self._configure(env)}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._details = lm_info['details']


# ------------------------------------------------------------------------------

