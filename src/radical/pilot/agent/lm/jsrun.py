
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

        slots         = cu['slots']
        cud           = cu['description']
        task_exec     = cud['executable']
        task_mpi      = bool('mpi' in cud.get('cpu_process_type', '').lower())
        task_procs    = cud.get('cpu_processes', 0)
        task_threads  = cud.get('cpu_threads', 0)
        task_gpu_pros = cud.get('gpu_processes', 0)
        task_env      = cud.get('environment') or dict()
        task_args     = cud.get('arguments')   or list()
        task_argstr   = self._create_arg_string(task_args)

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
        rs_id        = 0
        for node in slots['nodes']:
            hosts_string += 'RS %d : {host: %d ' %(rs_id,node['uid'])
            hosts_string += 'cpu: %s ' % ' '.join(map(str, node['core_map'][0]))
            hosts_string += 'cpu: %s ' % ' '.join(map(str, node['gpu_map'][0]))
            hosts_string += '}\n'

        with open('rs_layout_cu_%s' % cud['uid']) as rs_layout:
            rs_layout.write(hosts_string)
        
        flags   = '-U %d -n1 -a1 ' % ('rs_layout_cu_%s' % cud['uid'])
        command = '%s %s %s %s' % (self.launch_command, flags,
                                            env_string, 
                                            task_command)
        return command, None


# ------------------------------------------------------------------------------

