
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class JSRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('jsrun')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_mpi     = bool('mpi' in cud.get('cpu_process_type', '').lower())
        task_cores   = cud.get('cpu_processes', 0) + cud.get('gpu_processes', 0)
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

     #  import pprint
     #  self._log.debug('prep %s', pprint.pformat(cu))
        self._log.debug('prep %s', cu['uid'])

        if 'lm_info' not in slots:
            raise RuntimeError('No lm_info to launch via %s: %s'
                               % (self.name, slots))

        if not slots['lm_info']:
            raise RuntimeError('slots missing for %s: %s'
                               % (self.name, slots))

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:
            for var in env_list:
                env_string += '-x "%s" ' % var

        # Construct the hosts_string, env vars
        hosts_string = ''
        depths       = set()
        cores        = 0
        procs        = 0
        for node in slots['nodes']:

            # On some Crays, like on ARCHER, the hostname is "archer_N".  In
            # that case we strip off the part upto and including the underscore.
            #
            # TODO: If this ever becomes a problem, i.e. we encounter "real"
            #       hostnames with underscores in it, or other hostname 
            #       mangling, we need to turn this into a system specific 
            #       regexp or so.
            node_id = node[1].rsplit('_', 1)[-1] 

            # add all cpu and gpu process slots to the node list.
            for gpu_slot in node[3]: hosts_string += '%s,' % node_id
            for cpu_slot in node[2]: hosts_string += '%s,' % node_id
            for cpu_slot in node[2]: depths.add(len(cpu_slot))
            for cpu_slot in node[2]: cores += len(cpu_slot)
            for cpu_slot in node[2]: procs += 1

        assert(len(depths) == 1), depths
        depth = list(depths)[0]

        # remove trailing ','
        hosts_string = hosts_string.rstrip(',')

        flags   = '-n %d -g 1 -a %d -c16 -bpacked:%d' % (procs, procs * depth, depth)
        command = '%s %s -host %s %s %s' % (self.launch_command, flags,
                                            hosts_string, env_string, 
                                            task_command)
        return command, None


# ------------------------------------------------------------------------------

