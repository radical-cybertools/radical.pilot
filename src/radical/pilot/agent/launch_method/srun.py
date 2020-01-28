
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import math

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
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)
        sbox         = cu['unit_sandbox_path']

        # Construct the executable and arguments
        if task_argstr: task_cmd = "%s %s" % (task_exec, task_argstr)
        else          : task_cmd = task_exec

        env = ''
        env_list   = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:
            env = '--export="%s"' % ','.join(env_list)

        if not slots:
            # leave placement to srun
            ncores    = cud['cpu_threads']
            nprocs    = cud['cpu_processes']
            cpn       = self._cfg.get('cores_per_node', 1)
            nnodes    = int(math.ceil(nprocs / float(cpn)))

        else:
            # Extract all the hosts from the slots
            hostlist = list()
            uniform  = True
            chunk    = None
            for node in slots['nodes']:

                this_chunk = [len(node['core_map']),
                              len(node['gpu_map' ])]

                if not chunk:
                    chunk = this_chunk

                if chunk != this_chunk:
                    uniform = False
                    break

                for _ in node['core_map']:
                    hostlist.append(node['name'])

                for _ in node['gpu_map']:
                    hostlist.append(node['name'])

            if uniform:

                # we can attempt placement - flag it and prepare SLURM_HOSTFILE
                hostfile = '%s/slurm_hostfile' % sbox
                with open(hostfile, 'w') as fout:
                    fout.write(','.join(hostlist))
                    fout.write('\n')

                if not cu['description'].get('pre_exec'):
                    cu['description']['pre_exec'] = list()
                cu['description']['pre_exec'].append(
                                  'export SLURM_HOSTFILE="%s"' % hostfile)

            ncores = len(slots['nodes'][0]['core_map'][0])
            nnodes = len(set(hostlist))
            nprocs = len(hostlist)

        placement = '-N %d -n %d' % (nnodes, nprocs)
        if ncores > 1:
            placement += ' -c %d' % ncores

        cmd = '%s %s %s %s' % (self.launch_command, placement, env, task_cmd)
        return cmd, None


# ------------------------------------------------------------------------------

