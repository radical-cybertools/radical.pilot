
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
    def __init__(self, name, cfg, session):

        self._mpt    = False
        self._rsh    = False
        self._ccmrun = ''
        self._dplace = ''

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if '_rsh' in self.name.lower():
            self._rsh = True
            self.launch_command = ru.which(['mpirun_rsh',  # Gordon (SDSC)
                                            'mpirun'       # general case
                                           ])

        elif '_mpt' in self.name.lower():
            self._mpt = True
            self.launch_command = ru.which(['mpirun_mpt',  # Cheyenne (NCAR)
                                            'mpirun'       # general case
                                           ])
        else:
            self.launch_command = ru.which(['mpirun-mpich-mp',    # Mac OSX
                                            'mpirun-openmpi-mp',  # Mac OSX
                                            'mpirun',             # general case
                                           ])

        # cheyenne is special: it needs MPT behavior (no -host) even for the
        # default mpirun (not mpirun_mpt).
        if 'cheyenne' in self._cfg.resource.lower():
            self._mpt = True

        # don't use the full pathname as the user might load a different
        # compiler / MPI library suite from his Task pre_exec that requires
        # the launcher from that version -- see #572.
        # FIXME: then why are we doing this LM setup in the first place??
        if self.launch_command:
            self.launch_command = os.path.basename(self.launch_command)


        # do we need ccmrun or dplace?
        if '_ccmrun' in self.name:
            self._ccmrun = ru.which('ccmrun')
            if not self._ccmrun:
                raise RuntimeError("ccmrun not found!")

        if '_dplace' in self.name:
            self._dplace = ru.which('dplace')
            if not self._dplace:
                raise RuntimeError("dplace not found!")

        self.mpi_version, self.mpi_flavor = \
                                       self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        slots        = t['slots']
        uid          = t['uid']
        td          = t['description']
        sandbox      = t['task_sandbox_path']
        task_exec    = td['executable']
        task_threads = td.get('cpu_threads', 1)
        task_env     = td.get('environment') or dict()
        task_args    = td.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if '_dplace' in self.name and task_threads > 1:
            # dplace pinning would disallow threads to map to other cores
            raise ValueError('dplace can not place threads [%d]' % task_threads)

        # Construct the executable and arguments
        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:

            if self.mpi_flavor == self.MPI_FLAVOR_HYDRA:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif self.mpi_flavor == self.MPI_FLAVOR_OMPI:
                for var in env_list:
                    env_string += '-x "%s" ' % var

        # Cheyenne is the only machine that requires mpirun_mpt.  We then
        # have to set MPI_SHEPHERD=true
        if self._mpt:
            if not t['description'].get('environment'):
                t['description']['environment'] = dict()
            t['description']['environment']['MPI_SHEPHERD'] = 'true'

        # Extract all the hosts from the slots
        host_list = list()
        core_list = list()
        save_list = list()

        for node in slots['nodes']:

            for cpu_proc in node['core_map']:
                host_list.append(node['name'])
                core_list.append(cpu_proc[0])
                # FIXME: inform this proc about the GPU to be used

            if '_dplace' in self.name and save_list:
                assert(save_list == core_list), 'inhomog. core sets (dplace)'
            else:
                save_list = core_list

        if '_dplace' in self.name:
            self._dplace += ' -c '
            self._dplace += ','.join(core_list)


        # If we have a Task with many cores, we will create a hostfile and pass
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

        command = ("%s %s %s -np %d %s %s %s %s" %
                   (self._ccmrun, self.launch_command, mpt_hosts_string,
                    np, self._dplace, hosts_string, env_string,
                    task_command)).strip()

        return command, None


# ------------------------------------------------------------------------------

