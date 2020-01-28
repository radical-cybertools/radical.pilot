
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIRun_MPT(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which([
            'mpirun',            # General case
            'mpirun_rsh',        # Gordon @ SDSC
            'mpirun-mpich-mp',   # Mac OSX MacPorts
            'mpirun-openmpi-mp'  # Mac OSX MacPorts
        ])

        self.mpi_version, self.mpi_flavor = self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

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
        hosts_string = ",".join(hostlist)

        # On Cheyenne (which is the only machine that requires mpirun_mpt
        # currently), we also have to set MPI_SHEPHERD=true, we add it to the
        # environment of the cu which will get parsed in the spawner
        if 'environment' not in list(cu['description'].keys()):
            cu['description']['environment'] = dict()
        cu['description']['environment']['MPI_SHEPHERD'] = True

        # -np:  usually it is len(hostlist), as it means run N processes over
        # N hosts but for Cheyenne it is 1 since specification of the host means
        # land N processes on EACH host, where N is specified as arg to -np
        command = "%s %s -np %d %s %s" % (self.launch_command, hosts_string, 1,
                                          env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

