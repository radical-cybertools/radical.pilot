
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

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
    def _init_from_scratch(self, env, env_sh):
        '''
        The RP launch methods are used in different components: the agent will
        use them to spawn sub-agents and components, the resource manager may
        use them to spawn launcher services, and, of course, different execution
        components (including Raptor and other task overlays) can use them to
        launch tasks.

        The first use (likely in `agent.0`) will call this initializer to
        inspect LM properties.  Later uses will be able to use the information
        gathered and should re-initialize via `_init_from_info()`, using the
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

        mpi_version, mpi_flavor = self._get_mpi_info(lm_info['command'])
        lm_info['mpi_version']  = mpi_version
        lm_info['mpi_flavor']   = mpi_flavor

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

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

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        slots        = task['slots']
        uid          = task['uid']
        td           = task['description']
        sandbox      = task['task_sandbox_path']
        task_threads = td.get('cpu_threads', 1)

        if '_dplace' in self.name and task_threads > 1:
            # dplace pinning would disallow threads to map to other cores
            raise ValueError('dplace can not place threads [%d]' % task_threads)

        # Cheyenne is the only machine that requires mpirun_mpt.  We then
        # have to set MPI_SHEPHERD=true
        if self._mpt:
            if not task['description'].get('environment'):
                task['description']['environment'] = dict()
            task['description']['environment']['MPI_SHEPHERD'] = 'true'

        # Extract all the hosts from the slots
        host_list = list()
        core_list = list()
        save_list = list()

        for rank in slots['ranks']:

            for cpu_proc in rank['core_map']:
                host_list.append(rank['node_name'])
                core_list.append(cpu_proc[0])
                # FIXME: inform this proc about the GPU to be used

            if '_dplace' in self.name and save_list:
                assert(save_list == core_list), 'inhomog. core sets (dplace)'
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

        cmd = '%s %s %s -np %d %s %s %s %s' % \
            (self._ccmrun, self._command, mpt_hosts_string, np,
             self._dplace, self._omplace, hosts_string, exec_path)

        return cmd.strip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: we know the MPI flavor, so make this less guesswork

        ret  = 'test -z "$MPI_RANK"  || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK" || export RP_RANK=$PMIX_RANK\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

