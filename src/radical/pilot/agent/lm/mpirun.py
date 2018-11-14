
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

        if '_rsh' in self.name:
            self.launch_command = ru.which([
                'mpirun_rsh',        # Gordon @ SDSC
            ])

        else:
            self.launch_command = ru.which([
                'mpirun',            # General case
                'mpirun-mpich-mp',   # Mac OSX MacPorts
                'mpirun-openmpi-mp'  # Mac OSX MacPorts
            ])

        # don't use the full pathname as the user might load a different
        # compiler / MPI library suite from his CU pre_exec that requires
        # the launcher from that version -- see #572.
        self.launch_command = os.path.basename(self.launch_command)


        self.ccmrun_command = ''  # do *not* set to none - we use this in
                                  # a string completion later on

        # do we need ccmrun?
        if '_ccmrun' in self.name:
            self.ccmrun_command = ru.which('ccmrun')
            if not self.ccmrun_command:
                raise RuntimeError("ccmrun not found!")
            self.ccmrun_command += ' '  # simplifies later string replacement

        self.mpi_version, self.mpi_flavor = \
                                       self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        uid          = cu['uid']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

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

        if 'nodes' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        # Extract all the hosts from the slots
        hostlist = list()
        for node in slots['nodes']:
            for cpu_proc in node['core_map']:
                hostlist.append(node['name'])
            for gpu_proc in node['gpu_map']:
                hostlist.append(node['name'])

        # If we have a CU with many cores, we will create a hostfile and pass
        # that as an argument instead of the individual hosts
        if len(hostlist) > 42:

            # Create a hostfile from the list of hosts
            hostfile = self._create_hostfile(uid, hostlist, impaired=True)
            hosts_string = '-hostfile %s' % hostfile

        else:
            # Construct the hosts_string ('h1,h2,..,hN')
            hosts_string = '-host %s' % ",".join(hostlist)

        command = "%s%s -np %d %s %s %s" % \
                  (self.ccmrun_command, self.launch_command, len(hostlist),
                   hosts_string, env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

