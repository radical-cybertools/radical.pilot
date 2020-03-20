
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Srun(LaunchMethod):
    '''
    This launch method uses `srun` to place tasks into a slurm allocation.

    Srun has severe limitations compared to other launch methods, in that it
    does not allow to place a task on a specific set of nodes and cores, at
    least not in the general case.  It is possible to select nodes as long as
    the task uses (a part of) a single node, or the task is using multiple nodes
    uniformely.  Core pinning is only available on tasks which use exactly one
    full node (and in that case becomes useless for our purposes).

    We use srun in the following way:

        IF    task <= nodesize
        OR    task is uniformel
        THEN  enforce node placement
        ELSE  leave *all* placement to slurm
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('srun')

        out, err, ret = ru.sh_callout('%s -V' % self.launch_command)
        if ret:
            raise RuntimeError('cannot use srun [%s] [%s]' % (out, err))

        self._version = out.split()[1]
        self._log.debug('using srun from %s [%s]',
                        self.launch_command, self._version)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu.get('slots')
        uid          = cu['uid']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)
        sbox         = cu['unit_sandbox_path']

        # construct the task executable and arguments
        if task_argstr: task_cmd = "%s %s" % (task_exec, task_argstr)
        else          : task_cmd = task_exec

        # use `ALL` to export vars pre_exec and RP, and add task env explicitly
        env = '--export=ALL'
        for k,v in task_env.items():
            env += ",'%s'='%s'" % (k, v)


        # Alas, exact rank-to-core mapping seems only be availabe in Slurm when
        # tasks use full nodes - which in RP is rarely the case.  We thus are
        # limited to specifying the list of nodes we want the processes to be
        # placed on, and otherwise have to rely on the `--exclusive` flag to get
        # a decent auto mapping.  In cases where the scheduler did not place
        # the task we leave the node placement to srun as well.
        #
        # debug mapping
        os.environ['SLURM_CPU_BIND'] = 'verbose'

        n_tasks          = cud['cpu_processes']
        threads_per_task = cud['cpu_threads']
        gpus_per_task    = cud['gpu_processes']

        # use `--exclusive` to ensure all tasks get individual resources.
        mapping = '--exclusive --ntasks %d --cpus-per-task %s --gpus-per-task' \
                % (n_tasks, threads_per_task, gpus_per_task)

        if slots:

            # the scheduler *did* place tasks - at least honor the nodelist.
            nodelist = [node['name'] for node in slots['nodes']]
            nodefile = '%s/%s.nodes' % (sbox, uid)
            with open(nodefile, 'w') as fout:
                fout.write(','.join(nodelist))
                fout.write('\n')

            mapping += ' --nodelist=%s' % nodefile

        cmd = '%s %s %s %s' % (self.launch_command, mapping, env, task_cmd)
        return cmd, None


# ------------------------------------------------------------------------------

