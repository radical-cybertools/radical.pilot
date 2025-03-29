
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    class Partition(ru.TypedDict):

        _extensible = False
        _schema     = {'uid'    : str,
                       'uri'    : str,
                       'service': object,
                       'handle' : object}

        def as_dict(self, _annotate=False):

            return {'uid': self.uid,
                    'uri': self.uri}


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

            for fh in part.handles:
                try   : fh.stop()
                except: pass

        self._partitions = list()


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        self._prof.prof('flux_start')

        # ensure we have flux
        flux = ru.which('flux')
        self._log.debug('flux location: %s', flux)
        if not flux:
            raise RuntimeError('flux not found')

        n_partitions        = self._rm_info.n_partitions
        n_nodes             = len(self._rm_info.node_list)
        nodes_per_partition = int(n_nodes / n_partitions)
        threads_per_node    = self._rm_info.cores_per_node  # == hw threads
        gpus_per_node       = self._rm_info.gpus_per_node

        assert n_nodes % n_partitions == 0, \
                'n_nodes %d %% n_partitions %d != 0' % (n_nodes, n_partitions)

        self._log.info('using %d flux partitions [%d nodes]',
                       n_partitions, nodes_per_partition)

        fluxes  = list()
        for n in range(n_partitions):

            self._log.debug('flux partition %d starting', n)

            # FIXME: this is a hack for frontier and will only work for slurm
            #        resources.  If Flux is to be used more widely, we need to
            #        pull the launch command from the agent's resource manager.
            launcher = ''
            srun = ru.which('srun')
            if srun:
                launcher = 'srun -n %s -N %d --ntasks-per-node 1 ' \
                           '--cpus-per-task=%d --gpus-per-task=%d ' \
                           '--export=ALL' \
                           % (nodes_per_partition, nodes_per_partition,
                              threads_per_node, gpus_per_node)

            fs = ru.FluxService(launcher=launcher)
            fs.start()
            fluxes.append(fs)

        for flux in fluxes:

            if not flux.ready(timeout=60):
                raise RuntimeError('flux service did not start')

            self._log.debug('flux partition %s started', fs.uid)
            self._partitions.append(self.Partition(service=flux,
                                                   uri=fs.uri,
                                                   uid=fs.uid))


        lm_info = {'env'       : env,
                   'env_sh'    : env_sh,
                   'partitions': [part.as_dict() for part in self._partitions]}

        self._prof.prof('flux_start_ok')
        self._log.debug('flux partitions started: %s',
                        [part.uid for part in self.partitions])

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        self._prof.prof('flux_reconnect')

        self._env    = lm_info['env']
        self._env_sh = lm_info['env_sh']

        for pinfo in lm_info['partitions']:

            part = self.Partition(pinfo)
            part.handle = ru.FluxHelper(uri=part.uri)
            part.handle.start()

            self._log.debug('connect flux partition: %s [%s]', part.uri,
                            part.handle)

            self._partitions.append(part)

        self._prof.prof('flux_reconnect_ok')


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

