

__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class IBRun(LaunchMethod):

    node_list = None

    # NOTE: Don't think that with IBRUN it is possible to have
    #       processes != cores ...

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

        self._node_list = self._cfg.rm_info.node_list


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

        cpn     = self._cfg.cores_per_node
        index   = 0
        offsets = list()
        print('offs : %s' % offsets)

        import pprint
      # self._log.error('cpn  : %s', cpn)
      # self._log.error('slots: %s', pprint.pformat(slots))
        print('\n\ncpn  : %s' % cpn)
        print('slots: %s' % pprint.pformat(slots))
        for node in self._node_list:
            for slot_node in slots['nodes']:
                if slot_node['uid'] == node[0]:
                    for core_map in slot_node['core_map']:
                        for core_idx in core_map:
                            print('add 0 offset %s [%s + %s] %s' %
                                     (index + core_idx, index, core_idx, offsets))
                            offsets.append(index + core_idx)
                            print('add 1 offset %s [%s + %s] %s\n' %
                                     (index + core_idx, index, core_idx, offsets))

            print('inc index 0 %s %s'   % (index, offsets))
            index += cpn
            print('inc index 1 %s %s\n' % (index, offsets))

      # self._log.error('offs : %s', offsets)
        print('offs : %s' % offsets)
        ibrun_offset = min(offsets)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        ibrun_command = "%s -n %s -o %d %s" % \
                        (self.launch_command, task_cores,
                         ibrun_offset, task_command)
      # ibrun_command = "%s -n %s %s" % \
      #                 (self.launch_command, task_cores, task_command)

        return ibrun_command, None


# ------------------------------------------------------------------------------

