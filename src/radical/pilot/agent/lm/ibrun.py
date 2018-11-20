

__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class IBRun(LaunchMethod):

    # NOTE: Don't think that with IBRUN it is possible to have
    #       processes != cores ...

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # ibrun: wrapper for mpirun at TACC
        self.launch_command = ru.which('ibrun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes']  # FIXME: handle cpu_threads
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)
        cpn          = slots['lm_info']['cores_per_node']

        if not 'task_offsets' in slots:
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, slots))

        task_offsets = slots['task_offsets']        # This needs to revisited 
        assert(len(task_offsets) == 1)              # since slots structure has 
        ibrun_offset = task_offsets[0]              # changed

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        ibrun_command = "%s -n %s -o %d %s" % \
                        (self.launch_command, task_cores,
                         ibrun_offset, task_command)

        return ibrun_command, None


# ------------------------------------------------------------------------------

