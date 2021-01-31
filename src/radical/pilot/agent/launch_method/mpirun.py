
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lmcfg, cfg, session):

        self._mpt    = False
        self._rsh    = False
        self._ccmrun = ''
        self._dplace = ''

        LaunchMethod.__init__(self, name, lmcfg, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._lm_configs = self._cfg.resource_cfg.launch_methods
        self._lm_config  = self._lm_configs.get(self.name, {})
        self._pre_exec   = self._lm_config.get('pre_exec', [])

        print('pre: ', self._pre_exec)

        # set up LM environment: first recreate the agent's original env, then
        # apply the launch method's pre_exec commands.  Store the resulting env
        # in `env/lm_<name>.env`
        self._env_orig = ru.env_eval('env/orig.env')
        self._env_sh   = '%s/env/%s.env' % (self._pwd, self.name)
        self._env      = ru.env_prep(self._env_orig,
                                     pre_exec=self._pre_exec,
                                     script_path=self._env_sh)

        # find the path to the launch method executable, so that we can derive
        # launcher version and flavor

        if '_rsh' in self.name.lower():
            # mpirun_rsh : Gordon (SDSC)
            # mpirun     : general case
            self._rsh = True
            cmd = 'which mpirun_rsh || ' \
                  'which mpirun'
            out, err, ret = ru.sh_callout(cmd, shell=True, env=self._env)
            assert(not ret), err
            self.launch_command = out.strip()


        elif '_mpt' in self.name.lower():
            # mpirun_mpt: Cheyenne (NCAR)
            # mpirun    : general case
            self._mpt = True
            cmd = 'which mpirun_mpt || ' \
                  'which mpirun'
            out, err, ret = ru.sh_callout(cmd, shell=True, env=self._env)
            assert(not ret), err
            self.launch_command = out.strip()

        else:
            # mpirun-mpich-mp  : Mac OSX mpich
            # mpirun-openmpi-mp: Mac OSX openmpi
            # mpirun           : general case
            cmd = 'which mpirun_mpich_mp   || ' \
                  'which mpirun-openmpi-mp || ' \
                  'which mpirun'
            out, err, ret = ru.sh_callout(cmd, shell=True, env=self._env)
            assert(not ret), err
            self.launch_command = out.strip()


        # cheyenne is special: it needs MPT behavior (no -host) even for the
        # default mpirun (not mpirun_mpt).
        if 'cheyenne' in self._cfg.resource.lower():
            self._mpt = True

        # which will return the first match in path - dispense of the path part
        if self.launch_command:
            self.launch_command = os.path.basename(self.launch_command)

        # do we need ccmrun or dplace?
        if '_ccmrun' in self.name:
            out, err, ret = ru.sh_callout('which ccmrun', shell=True, env=self._env)
            assert(not ret), err
            ccmrun = out.strip()
            if not ccmrun:
                raise RuntimeError("ccmrun not found!")
            self.launch_command = '%s %s' % (ccmrun, self.launch_command)

        if '_dplace' in self.name:
            out, err, ret = ru.sh_callout('which dplace', shell=True, env=self._env)
            assert(not ret), err
            self._dplace = out.strip()
            if not self._dplace:
                raise RuntimeError("dplace not found!")

        self.mpi_version, self.mpi_flavor = \
                                       self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if task['description']['cpu_process_type'] == 'MPI':
            return True

        return False


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. %s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def get_task_env(self, spec):

        name = spec['name']
        cmds = spec['cmds']

        # we assume that the launcher env is still active in the task execution
        # script.  We thus remove the launcher env from the task env before
        # applying the task env's pre_exec commands
        tgt = '%s/env/%s.env' % (self._pwd, name)
        self._log.debug('=== tgt : %s', tgt)

        if not os.path.isfile(tgt):

            # the env does not yet exists - create
            ru.env_prep(self._env_orig,
                        unset=self._env,
                        pre_exec=cmds,
                        script_path=tgt)

        return tgt


    # --------------------------------------------------------------------------
    #
    def get_launch_command(self, task, exec_script):

        slots        = task['slots']
        uid          = task['uid']
        td           = task['description']
        sandbox      = task['unit_sandbox_path']
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
                host_list.append(rank['node'])
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
            hostfile = self._create_hostfile(sandbox, uid, host_list,
                                             impaired=True)
            if self._mpt: hosts_string = '-file %s'     % hostfile
            else        : hosts_string = '-hostfile %s' % hostfile

        else:
            # Construct the hosts_string ('h1,h2,..,hN')
            if self._mpt: mpt_hosts_string = '%s'       % ",".join(host_list)
            else        : hosts_string     = '-host %s' % ",".join(host_list)

        # -np:  usually len(host_list), meaning N processes over N hosts, but
        # for Cheyenne (mpt) the specification of -host lands N processes on
        # EACH host, where N is specified as arg to -np
        if self._mpt: np = 1
        else        : np = len(host_list)

        command = ("%s %s -np %d %s %s %s" %
                   (self.launch_command, mpt_hosts_string, np, self._dplace,
                                         hosts_string, exec_script))

        return command


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
        command      = "%s %s" % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

