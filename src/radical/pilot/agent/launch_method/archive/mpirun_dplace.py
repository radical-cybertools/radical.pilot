
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod



# ------------------------------------------------------------------------------
#
# dplace: job launcher for SGI systems (e.g. on Blacklight)
# https://www.nas.nasa.gov/hecc/support/kb/Using-SGIs-dplace-Tool-for-Pinning_284.html
#
class MPIRunDPlace(LaunchMethod):

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

        self.dplace_command = ru.which([
            'dplace',            # General case
        ])

        if not self.dplace_command:
            raise RuntimeError("mpirun not found!")

        self.mpi_version, self.mpi_flavor = self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_threads = cud['cpu_thread']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

      # if task_threads > 1:
      #     # dplace pinning would disallow threads to map to other cores
      #     raise ValueError('dplace can not place threads [%d]' % task_threads)

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

        host_list = list()
        core_list = list()
        for node in slots['nodes']:
            tmp_list = list()
            for cpu_proc in node['core_map']:
                host_list.append(node['name'])
                tmp_list.append(cpu_proc[0])
            for gpu_proc in node['gpu_map']:
                host_list.append(node['name'])
                tmp_list.append(gpu_proc[0])

            if core_list:
                if sorted(core_list) != sorted(tmp_list):
                    raise ValueError('dplace expects homoogeneous layouts')
            else:
                core_list = tmp_list

        np          = len(host_list) + len(core_list)
        host_string = ",".join(host_list)
        core_string = ','.join(core_list)


        command = "%s -h %s -np %d %s -s %d -c %s %s %s" % \
                  (self.launch_command, host_string, np,
                   self.dplace_command, np, core_string,
                   env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

