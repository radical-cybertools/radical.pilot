
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    class Partition(object):

        def __init__(self):

            self.uid      = None
            self.uri      = None
            self.service  = None
            self.helper   = None


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, session, prof):

        self._partitions = list()

        super().__init__(name, lm_cfg, rm_info, session, prof)


    # --------------------------------------------------------------------------
    #
    def _terminate(self):

        for part in self._partitions:

            self._log.debug('stop partition %s [%s]', part.uid, part.uri)

            try   : part.service.stop()
            except: pass

            for fh in part.helper:
                try   : fh.stop()
                except: pass

        self._partitions = list()


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        # just make sure we can load flux

        flux, flux_job, flux_exc, flux_v = ru.flux.import_flux()
        if flux_exc:
            raise RuntimeError('flux import failed: %s' % flux_exc)

        self._log.debug('=== flux import : %s', flux)
        self._log.debug('=== flux job    : %s', flux_job)
        self._log.debug('=== flux version: %s', flux_v)

        # ensure we have flux
        flux = ru.which('flux')
        self._log.debug('=== flux location: %s', flux)
        if not flux:
            raise RuntimeError('flux not found')

        lm_info = {'env'       : env,
                   'env_sh'    : env_sh}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def start_flux(self):

        self._prof.prof('flux_start')

        n_partitions        = self._rm_info.n_partitions
        n_nodes             = len(self._rm_info.node_list)
        nodes_per_partition = int(n_nodes / n_partitions)
        threads_per_node    = self._rm_info.cores_per_node  # == hw threads
        gpus_per_node       = self._rm_info.gpus_per_node

        assert n_nodes % n_partitions == 0, \
                'n_nodes %d %% n_partitions %d != 0' % (n_nodes, n_partitions)

        self._log.info('using %d flux partitions [%d nodes]',
                       n_partitions, nodes_per_partition)

        launcher = ''
        for n in range(n_partitions):

            part = self.Partition()

            self._log.debug('flux partition %d starting', n)

            # FIXME: this is a hack for frontier and will only work for slurm
            #        resources.  If Flux is to be used more widely, we need to
            #        pull the launch command from the agent's resource manager.
            srun = ru.which('srun')
            if srun:
                launcher = 'srun -n %s -N %d --ntasks-per-node 1 ' \
                           '--cpus-per-task=%d --gpus-per-task=%d ' \
                           '--export=ALL' \
                           % (nodes_per_partition, nodes_per_partition,
                              threads_per_node, gpus_per_node)

            part.service = ru.FluxService(launcher=launcher)
            part.service.start()
            self._partitions.append(part)

        for part in self._partitions:

            if not part.service.ready(timeout=600):
                raise RuntimeError('flux service did not start')

            # check if we shuld use the remote uri
            if launcher and n_partitions > 1:
                part.uri = part.service.r_uri
            else:
                part.uri = part.service.uri

            self._log.debug('flux partition %s: %s', part.service.uid, part.uri)

            part.helper = ru.FluxHelper(uri=part.uri)
            part.helper.start()

            self._log.debug('flux helper    %s', part.helper.uid)

        self._prof.prof('flux_start_ok')
        self._log.debug('flux partitions started: %s',
                        [part.uid for part in self.partitions])



    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        pass


    # --------------------------------------------------------------------------
    #
    @property
    def partitions(self):
        return self._partitions


    def can_launch(self, task):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launch_cmds(self, task, exec_path):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launcher_env(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_cmd(self):
        return 'export RP_RANK=$FLUX_TASK_RANK\n'


# ------------------------------------------------------------------------------

