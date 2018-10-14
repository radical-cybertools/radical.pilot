
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
    def _create_resource_set_file(self, slots, cuid):
        """
        This method takes as input a CU slots and creates the necessary
        resource set file. This resource set file is then use by jsrun to 
        place and execute tasks on nodes.

        An example of a resource file is:

        *Task 1: 2 MPI procs, 2 threads per process and 2 gpus per process*
        RS 0 : {host: 1 cpu: 0 1 gpu: 0 1}
        RS 1 : {host: 1 cpu: 22 23 gpu: 3 4}

        *Task 2: 2 MPI procs, 1 thread per process and 1 gpus per process*
        RS 0 : {host: 2 cpu: 7 gpu: 2}
        RS 1 : {host: 2 cpu: 30 gpu: 5}

        Parameters
        ----------
        slots : List of dictionaries.
            The slots that the unit will be placed. A slot has the following
            format:
            {"cores_per_node": 16,
             "lfs_per_node": {"size": 0, "path": "/dev/null"},
             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                        "core_map": [[0]],
                        "name": "a",
                        "gpu_map": [],
                        "uid": 1}],
             "lm_info": "INFO",
             "gpus_per_node": 6
            }

        id : int
            The ID of the unit.
        """

        node_ids = list()
        core_maps = list()
        gpu_maps = list()
        for node in slots['nodes']:
            node_ids += [node['uid']] * len(node['core_map'])
        core_maps = [core_map for core_map in node['core_map'] for node in slots['nodes']]
        gpu_maps = [gpu_map for gpu_map in node['gpu_map'] for node in slots['nodes']]
        rs_id = 0
        rs_str = ''
        if len(gpu_maps):
            for node_id,core_ids,gpu_ids in zip(node_ids, core_maps, gpu_maps):
                rs_str += 'RS %d : {' % rs_id
                rs_str += 'host: %d ' % node_id
                rs_str += 'cpu: %s ' % ' '.join(map(str,core_ids))
                rs_str += 'gpu: %s}\n' % ' '.join(map(str,gpu_ids))
                rs_id += 1
        else:
            for node_id,core_ids in zip(node_ids, core_maps):
                rs_str += 'RS %d : {' % rs_id
                rs_str += 'host: %d ' % node_id
                rs_str += 'cpu: %s ' % ' '.join(map(str,core_ids))
                rs_str += '}\n'
                rs_id += 1
        
        rs_name = 'rs_layout_cu_%06d' % cuid
        with open(rs_name, 'w') as rs_file:
            rs_file.write(rs_str)      


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots          = cu['slots']
        cud            = cu['description']
        task_exec      = cud['executable']
        task_mpi       = bool('mpi' in cud.get('cpu_process_type', '').lower())
        task_procs     = cud.get('cpu_processes', 0)
        task_threads   = cud.get('cpu_threads', 0)
        task_gpu_procs = cud.get('gpu_processes', 0)
        task_env       = cud.get('environment') or dict()
        task_args      = cud.get('arguments')   or list()
        task_argstr    = self._create_arg_string(task_args)

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

        self._create_resource_set_file(slots = slots, cuid = cu['uid'])

        flags   = '-U %s -n%d -a%d ' % ('rs_layout_cu_%06d' % cu['uid'], 
                                        task_procs, task_procs)
        command = '%s %s %s %s' % (self.launch_command, flags,
                                            env_string, 
                                            task_command)
        return command, None


# ------------------------------------------------------------------------------

