
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, cfg, log, prof):

        self._mpt    : bool  = False
        self._rsh    : bool  = False
        self._ccmrun : str   = ''
        self._omplace: str   = ''
        self._dplace : str   = ''
        self._command: str   = ''

        self._env_orig = ru.env_eval('env/bs0_orig.env')

        LaunchMethod.__init__(self, name, lm_cfg, cfg, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, lm_cfg, env, env_sh):

        lm_info = {'env'   : env,
                   'env_sh': env_sh}

        lm_info['command'] = ru.which([
            'mpiexec',             # General case
            'mpiexec.mpich',       # Linux, MPICH
            'mpiexec.hydra',       # Linux, MPICH
            'mpiexec.openmpi',     # Linux, MPICH
            'mpiexec-mpich-mp',    # Mac OSX MacPorts
            'mpiexec-openmpi-mp',  # Mac OSX MacPorts
            'mpiexec_mpt',         # Cheyenne (NCAR)
        ])

        if '_mpt' in self.name.lower():
            lm_info['mpt'] = True
        else:
            lm_info['mpt'] = False

        if '_rsh' in self.name.lower():
            lm_info['rsh'] = True
        else:
            lm_info['rsh'] = False

        lm_info['omplace'] = False
        # cheyenne always needs mpt and omplace
        if 'cheyenne' in ru.get_hostname():
            lm_info['mpt']     = True
            lm_info['omplace'] = True

        # do we need ccmrun or dplace?
        lm_info['ccmrun'] = ''
        if '_ccmrun' in self.name:
            lm_info['ccmrun'] = ru.which('ccmrun')
            assert(lm_info['ccmrun'])

        lm_info['dplace'] = ''
        if '_dplace' in self.name:
            lm_info['dplace'] = ru.which('dplace')
            assert(lm_info['dplace'])

        mpi_version, mpi_flavor = self._get_mpi_info(lm_info['command'])
        lm_info['mpi_version']  = mpi_version
        lm_info['mpi_flavor']   = mpi_flavor

        self._init_from_info(lm_info, lm_cfg)

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info, lm_cfg):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']

        self._mpt         = lm_info['mpt']
        self._rsh         = lm_info['rsh']
        self._dplace      = lm_info['dplace']
        self._omplace     = lm_info['omplace']
        self._ccmrun      = lm_info['ccmrun']
        self._command     = lm_info['command']

        self._mpi_version = lm_info['mpi_version']
        self._mpi_flavor  = lm_info['mpi_flavor']


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if task['description']['cpu_process_type'] == 'MPI':
            return True

        return False



    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]


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
    def get_launch_cmds(self, task, exec_script):

        slots = task['slots']

        if 'ranks' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        # extract a map of hosts and #slots from slots.  We count cpu
        # slot sets, but do not account for threads.  Since multiple slots
        # entries can have the same node names, we *add* new information.
        host_slots = dict()
        for rank in slots['ranks']:
            node_name = rank['node']
            if node_name not in host_slots:
                host_slots[node_name] = 0
            host_slots[node_name] += len(rank['core_map'])

        # If we have a Task with many cores, and the compression didn't work
        # out, we will create a hostfile and pass that as an argument
        # instead of the individual hosts.  The hostfile has this format:
        #
        #   node_name_1 slots=n_1
        #   node_name_2 slots=n_2
        #   ...
        #
        # where the slot number is the number of processes we intent to spawn.
        #
        # For smaller node sets, we construct command line parameters as
        # clusters of nodes with the same number of processes, like this:
        #
        #   -host node_name_1,node_name_2 -n 8 : -host node_name_3 -n 4 : ...
        #
        # POSIX defines a min argument limit of 4096 bytes, so we try to
        # construct a command line, and switch to hostfile if that limit is
        # exceeded.  We assume that Disk I/O for the hostfile is much larger
        # than the time needed to construct the command line.
        arg_max = 4096

        # This is the command w/o the host string
        omplace = ''
        if self._omplace:
            omplace = 'omplace'
        command_stub = "%s %%s %s %s" % (self._command, omplace, exec_script)

        # cluster hosts by number of slots
        host_string = ''
        if not self._mpt:
            for node,nslots in list(host_slots.items()):
                host_string += '-host '
                host_string += '%s -n %s ' % (','.join([node] * nslots), nslots)
        else:
            hosts = list()
            for node,nslots in list(host_slots.items()):
                hosts += [node] * nslots
            host_string += ','.join(hosts)
            host_string += ' -n 1'

        command = command_stub % host_string

        if len(command) > arg_max:

            # Create a hostfile from the list of hosts.  We create that in the
            # task sandbox
            hostfile = '%s/mpi_hostfile' % t['task_sandbox_path']
            with open(hostfile, 'w') as f:
                for node,nslots in list(host_slots.items()):
                    f.write('%20s \tslots=%s\n' % (node, nslots))
            host_string = "-hostfile %s" % hostfile

        command = command_stub % host_string
        self._log.debug('mpiexec cmd: %s', command)

        assert(len(command) <= arg_max)

        ret = ''

        # Cheyenne is the only machine that requires mpirun_mpt.  We then
        # have to set MPI_SHEPHERD=true
        if self._mpt:
            ret = 'export MPI_SHEPHERD=true\n%s' % command
        else:
            ret = command

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

