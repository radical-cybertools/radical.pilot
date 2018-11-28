
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class MPIRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if   '_rsh' in self.name:
            self.launch_command = ru.which('mpirun_rsh')  # Gordon   @ SDSC

        elif '_mpt' in self.name:
            self.launch_command = ru.which('mpirun_mpt')  # Cheyenne @ NCAR

        else:
            self.launch_command = ru.which(['mpirun',            # general case
                                            'mpirun-mpich-mp',   # Mac OSX
                                            'mpirun-openmpi-mp'  # Mac OSX
                                           ])

        # don't use the full pathname as the user might load a different
        # compiler / MPI library suite from his CU pre_exec that requires
        # the launcher from that version -- see #572.
        self.launch_command = os.path.basename(self.launch_command)


        # do we need ccmrun or dplace?
        self.ccmrun_command = ''
        if '_ccmrun' in self.name:
            self.ccmrun_command = ru.which('ccmrun')
            if not self.ccmrun_command:
                raise RuntimeError("ccmrun not found!")

        self.dplace_command = ''
        if '_dplace' in self.name:
            self.dplace_command = ru.which('dplace')
            if not self.dplace_command:
                raise RuntimeError("dplace not found!")

        self.mpi_version, self.mpi_flavor = \
                                       self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        uid          = cu['uid']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_threads = cud.get('cpu_threads', 1)
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if '_dplace' in self.name and task_threads > 1:
            # dplace pinning would disallow threads to map to other cores
            raise ValueError('dplace can not place threads [%d]' % task_threads)

        # Construct the executable and arguments
        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:

            if self.mpi_flavor == self.MPI_FLAVOR_HYDRA:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif self.mpi_flavor == self.MPI_FLAVOR_OMPI:
                for var in env_list:
                    env_string += '-x "%s" ' % var

        # Cheyenne is the only machine that requires mpirun_mpt.  We then 
        # have to set MPI_SHEPHERD=true
        if '_mpt' in self.name:
            if not cu.get['description'].get('environment'):
                cu['description']['environment'] = dict()
            cu['description']['environment']['MPI_SHEPHERD'] = 'true'

        # Extract all the hosts from the slots
        host_list = list()
        core_list = list()
        save_list = list()
        for node in slots['nodes']:
            for cpu_proc in node['core_map']:
                host_list.append(node['name'])
                core_list.append(cpu_proc[0])
            for gpu_proc in node['gpu_map']:
                host_list.append(node['name'])
                core_list.append(gpu_proc[0])

            if '_dplace' in self.name and save_list:
                assert(save_list == core_list), 'inhomog. core sets (dplace)'
            else:
                save_list = core_list

        if '_dplace' in self.name:
            self.dplace_command += ' -c '
            self.dplace_command += ','.join(core_list)


        # If we have a CU with many cores, we will create a hostfile and pass
        # that as an argument instead of the individual hosts
        if len(host_list) > 42:

            # Create a hostfile from the list of hosts
            hostfile = self._create_hostfile(uid, host_list, impaired=True)
            hosts_string = '-hostfile %s' % hostfile

        else:
            # Construct the hosts_string ('h1,h2,..,hN')
            hosts_string = '-host %s' % ",".join(host_list)

        # -np:  usually len(host_list), meaning N processes over N hosts, but
        # for Cheyenne (mpt) the specification of -host lands N processes on
        # EACH host, where N is specified as arg to -np
        if '_mpt' in self.name:
            np = 1
        else:
            np = len(host_list)

        command = ("%s %s -np %d %s %s %s %s" %
                   (self.ccmrun_command, self.launch_command, np,
                    self.dplace_command, hosts_string, env_string, 
                    task_command)).strip()

        return command, None


# ------------------------------------------------------------------------------

