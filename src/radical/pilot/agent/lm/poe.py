
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class POE(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # poe: LSF specific wrapper for MPI (e.g. yellowstone)
        self.launch_command = ru.which('poe')

        if not self.launch_command:
            raise RuntimeError("rsh not found!")


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
      # task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        if 'nodes' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                    % (self.name, slots))

        # Count slots per host in provided slots description.
        # Create string with format: "host_1 N  host_2 M  ..."
        hosts_string = ''
        for node in slots['nodes']:
                                     # nodename, n cores      + n gpus
            hosts_string += '%s %d ' % (node['name'], len(node['core_map']) + len(node['gpu_map']))

        # Override the LSB_MCPU_HOSTS env variable as this is set by
        # default to the size of the whole pilot.
        command = 'LSB_MCPU_HOSTS="%s" %s %s' \
                % (hosts_string, self.launch_command, task_command)

        return command, None


# ------------------------------------------------------------------------------

