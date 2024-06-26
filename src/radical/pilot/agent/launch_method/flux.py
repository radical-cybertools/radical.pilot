
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import threading     as mt

import radical.utils as ru

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

        self._log.debug('=== flux init from scratch')
        self._prof.prof('flux_start')

        n_partitions        = self._rm_info.details.n_partitions
        n_nodes             = len(self._rm_info.node_list)
        nodes_per_partition = int(n_nodes / n_partitions)
        threads_per_node    = self._rm_info.cores_per_node  # == hw threads
        gpus_per_node       = self._rm_info.gpus_per_node

        assert n_nodes % n_partitions == 0, \
                'n_nodes %d %% n_partitions %d != 0' % (n_nodes, n_partitions)

        self._log.info('using %d flux partitions [%d nodes]',
                       n_partitions, nodes_per_partition)

        threads = list()

        for n in range(n_partitions):

            thread = mt.Thread(target=self._init_partition,
                               args=[n, nodes_per_partition, threads_per_node,
                                     gpus_per_node])
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        self._prof.prof('flux_start_ok')

        lm_info = {'env'          : env,
                   'env_sh'       : env_sh,
                   'n_partitions' : n_partitions,
                   'details'      : self._details}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_partition(self, n, nodes_per_partition, threads_per_node,
                        gpus_per_node):

        fh = ru.FluxHelper()

        self._log.debug('=== starting flux partition %d', n)

        # FIXME: this is a hack for frontier and will only work for slurm
        #        resources.  If Flux is to be used more widely, we need to
        #        pull the launch command from the agent's resource manager.
        launcher = ''
        srun = ru.which('srun')
        if srun:
            launcher = 'srun -n %s -N %d --ntasks-per-node 1 --cpus-per-task=%d --gpus-per-task=%d --export=ALL' \
                       % (nodes_per_partition, nodes_per_partition, threads_per_node, gpus_per_node)

        self._log.debug('=== flux partition %d launcher: %s', n, launcher)

        fh.start_flux(launcher=launcher)

        self._flux_handles.append(fh)
        self._details.append({'flux_uri': fh.uri,
                              'flux_env': fh.env,
                              'flux_uid': fh.uid})

        self._log.debug('=== flux partition %d started', n)


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._log.debug('=== flux init from info')
        self._prof.prof('flux_reconnect')

        self._env          = lm_info['env']
        self._env_sh       = lm_info['env_sh']
        self._details      = lm_info['details']
        self._n_partitions = lm_info['n_partitions']

        threads = list()
        for details in self._details:

            thread = mt.Thread(target=self._reconnect, args=[details])
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        self._prof.prof('flux_reconnect_ok')

    # --------------------------------------------------------------------------
    #
    def _reconnect(self, details):

        fh = ru.FluxHelper(uid=details['flux_uid'])
        fh.connect_flux(uri=details['flux_uri'])
        self._flux_handles.append(fh)


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

