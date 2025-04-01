
__copyright__ = 'Copyright 2016-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._mpt    : bool  = False
        self._rsh    : bool  = False
        self._ccmrun : str   = ''
        self._dplace : str   = ''
        self._omplace: str   = ''
        self._command: str   = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):
        '''
        The RP launch methods are used in different components: the agent will
        use them to spawn sub-agents and components, the resource manager may
        use them to spawn launcher services, and, of course, different execution
        components (including Raptor and other task overlays) can use them to
        launch tasks.

        The first use (likely in `agent_0`) will call this initializer to
        inspect LM properties.  Later uses will be able to use the information
        gathered and should re-initialize via `init_from_info()`, using the
        info dict returned here.
        '''

        lm_info = {
            'env'    : env,
            'env_sh' : env_sh,
            'command': ru.which([
                'mpirun',             # general case
                'mpirun_rsh',         # Gordon (SDSC)
                'mpirun_mpt',         # Cheyenne (NCAR)
                'mpirun-mpich-mp',    # Mac OSX mpich
                'mpirun-openmpi-mp',  # Mac OSX openmpi
            ]),
            'mpt'    : False,
            'rsh'    : False,
            'ccmrun' : '',
            'dplace' : '',
            'omplace': ''
        }

        if not lm_info['command']:
            raise ValueError('mpirun not found - cannot start MPI tasks')

        if '_mpt' in self.name.lower():
            lm_info['mpt'] = True

        if '_rsh' in self.name.lower():
            lm_info['rsh'] = True

        # do we need ccmrun or dplace?
        if '_ccmrun' in self.name.lower():
            lm_info['ccmrun'] = ru.which('ccmrun')
            assert lm_info['ccmrun']

        if '_dplace' in self.name.lower():
            lm_info['dplace'] = ru.which('dplace')
            assert lm_info['dplace']

        # cheyenne always needs mpt and omplace
        if 'cheyenne' in ru.get_hostname():
            lm_info['omplace'] = ru.which('omplace')
            lm_info['mpt']     = True

        try:
            mpi_version, mpi_flavor = self._get_mpi_info(lm_info['command'])
            lm_info['mpi_version']  = mpi_version
            lm_info['mpi_flavor']   = mpi_flavor
            self._log.debug('got mpi version: %s (%s)', mpi_version, mpi_flavor)
        except:
            self._log.exception('failed to inspect MPI version and flavor')

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']
        self._command     = lm_info['command']

        assert self._command

        self._mpt         = lm_info['mpt']
        self._rsh         = lm_info['rsh']
        self._ccmrun      = lm_info['ccmrun']
        self._dplace      = lm_info['dplace']
        self._omplace     = lm_info['omplace']

        self._mpi_version = lm_info['mpi_version']
        self._mpi_flavor  = lm_info['mpi_flavor']


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        lm_env_cmds = ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]

        # Cheyenne is the only machine that requires mpirun_mpt.
        # We then have to set MPI_SHEPHERD=true
        if self._mpt:
            lm_env_cmds.append('export MPI_SHEPHERD=true')

        return lm_env_cmds


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        slots      = task['slots']
        uid        = task['uid']
        td         = task['description']
        sandbox    = task['task_sandbox_path']
        task_cores = td.get('cores_per_rank', 1)
        task_gpus  = td.get('gpus_per_rank', 0.)

        if '_dplace' in self.name and task_cores > 1:
            # dplace pinning would disallow threads to map to other cores
            raise ValueError('dplace can not place threads [%d]' % task_cores)

        # Extract all the hosts from the slots
        host_list = list()
        core_list = list()
        save_list = list()

        for slot in slots:

            host_list.append(slot['node_name'])
            core_list.append(slot['cores'][0])
            # FIXME: inform this proc about the GPU to be used

            if '_dplace' in self.name and save_list:
                assert (save_list == core_list), 'inhomog. core sets (dplace)'

            else:
                save_list = core_list

        if '_dplace' in self.name:
            self._dplace += ' -c '
            self._dplace += ','.join(core_list)


        # If we have a task with many cores, we will create a hostfile and pass
        # that as an argument instead of the individual hosts
        hosts_string     = ''
        mpt_hosts_string = ''
        if len(host_list) > 42:

            # Create a hostfile from the list of hosts
            hostfile = ru.create_hostfile(sandbox, uid, host_list,
                                          impaired=True)
            if self._mpt: hosts_string = '-file %s'     % hostfile
            else        : hosts_string = '-hostfile %s' % hostfile

        else:
            # Construct the hosts_string ('h1,h2,..,hN')
            if self._mpt: mpt_hosts_string = '%s'       % ','.join(host_list)
            else        : hosts_string     = '-host %s' % ','.join(host_list)

        # -np:  usually len(host_list), meaning N processes over N hosts, but
        # for Cheyenne (mpt) the specification of -host lands N processes on
        # EACH host, where N is specified as arg to -np
        if self._mpt: np = 1
        else        : np = len(host_list)

        options = ''
        if task_gpus and self._mpi_flavor == self.MPI_FLAVOR_SPECTRUM:
            # https://on-demand.gputechconf.com/gtc/2018/presentation/
            # s8314-multi-gpu-programming-with-mpi.pdf - s.18
            options = '-gpu '

        options += '-np %d' % np

        cmd = '%s %s %s %s %s %s %s %s' % \
            (self._ccmrun, self._command, mpt_hosts_string, options,
             self._dplace, self._omplace, hosts_string, exec_path)

        return cmd.strip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: we know the MPI flavor, so make this less guesswork

        ret  = 'test -z "$MPI_RANK"     || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK"    || export RP_RANK=$PMIX_RANK\n'

        if self._mpt:
            ret += 'test -z "$MPT_MPI_RANK" || export RP_RANK=$MPT_MPI_RANK\n'

        if self._mpi_flavor == self.MPI_FLAVOR_HYDRA:
            ret += 'test -z "$PMI_ID"       || export RP_RANK=$PMI_ID\n'
            ret += 'test -z "$PMI_RANK"     || export RP_RANK=$PMI_RANK\n'
        elif self._mpi_flavor == self.MPI_FLAVOR_PALS:
            ret += 'test -z "$PALS_RANKID"  || export RP_RANK=$PALS_RANKID\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_env_blacklist(self):
        '''
        extend the base blacklist by SLURM specific environment variables
        '''

        ret = super().get_env_blacklist()

        ret.extend([
                    'OMPI_*',
                    'PMIX_*',
                   ])

        return ret


# ------------------------------------------------------------------------------

