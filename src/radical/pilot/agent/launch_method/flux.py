
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

        self._flux_handles = list()
        self._details      = list()

        super().__init__(name, lm_cfg, rm_info, session, prof)

    # --------------------------------------------------------------------------
    #
    def _terminate(self):

        for fh in self._flux_handles:
            fh.reset()


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        sys_cfg = self._rm_info.rcfg.get('system_architecture', dict())
        self._n_partitions = sys_cfg.get('n_partitions', 1)

        for n in range(self._n_partitions):

            self._prof.prof('flux_start')
            fh = ru.FluxHelper()

            self._log.debug('starting flux')

            # FIXME: this is a hack for frontier and will onlu work for slurm
            #        resources.  If Flux is to be used more widely, we need to
            #        pull the launch command from the agent's resource manager.
            launcher = ''
            out, err, ret = ru.sh_callout('which srun')
            if ret == 0 and 'srun' in out:
                launcher = 'srun'

            fh.start_flux(launcher=launcher)

            self._flux_handles.append(fh)
            self._details.append({'flux_uri': fh.uri,
                                  'flux_env': fh.env})

        self._prof.prof('flux_start_ok')

        lm_info = {'env'          : env,
                   'env_sh'       : env_sh,
                   'n_partitions' : self._n_partitions,
                   'details'      : self._details}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._prof.prof('flux_reconnect')

        self._env          = lm_info['env']
        self._env_sh       = lm_info['env_sh']
        self._details      = lm_info['details']
        self._n_partitions = lm_info['n_partitions']

        for details in self._details:

            fh = ru.FluxHelper()
            fh.connect_flux(uri=details['flux_uri'])
            self._flux_handles.append(fh)

        self._prof.prof('flux_reconnect_ok')


    # --------------------------------------------------------------------------
    #
    def get_partition(self, partition):

        assert partition < self._n_partitions
        return self._flux_handles[partition]


    @property
    def n_partitions(self):
        return self._n_partitions

    def can_launch(self, task):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launch_cmds(self, task, exec_path):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launcher_env(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_cmd(self):

        return 'export RP_RANK=$FLUX_TASK_RANK\n'


# ------------------------------------------------------------------------------

