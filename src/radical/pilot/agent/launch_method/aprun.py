
__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class APRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('aprun')
        # only one instance of `aprun` is allowed per node at any time,
        # otherwise multiple instances will be executed sequentially

    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        td        = t['description']

        task_exec = td['executable']
        task_args = td['arguments']

        ppn = os.environ.get('SAGA_PPN')
        if ppn:
            ppn = min(td['cpu_processes'], int(ppn))
        else:
            ppn = td['cpu_processes']

        cmd_options = '-N %s ' % ppn + \
                      '-n %s ' % td['cpu_processes'] + \
                      '-d %s'  % td['cpu_threads']

        # CPU affinity binding
        # - use –d and --cc depth to let ALPS control affinity
        # - use --cc none if you want to use OpenMP (or KMP) env. variables
        #   to specify affinity: --cc none -e KMP_AFFINITY=<affinity>
        #   (*) turn off thread affinity: export KMP_AFFINITY=none
        #
        # saga_smt = os.environ.get('RADICAL_SAGA_SMT')
        # if saga_smt:
        #     cmd_options += ' -j %s' % saga_smt
        #     cmd_options += ' --cc depth'

        # `share` mode access restricts the application specific cpuset
        # contents to only the application reserved cores and memory on NUMA
        # node boundaries, meaning the application will not have access to
        # cores and memory on other NUMA nodes on that compute node.
        #
        # nodes = set([x['name'] for x in t['slots'].get('nodes', [])])
        # if len(nodes) < 2:
        #     cmd_options += ' -F share'  # default is `exclusive`
        # cmd_options += ' -L %s ' % ','.join(nodes)

        # task_env = td['environment']
        # cmd_options += ''.join([' -e %s=%s' % x for x in task_env.items()])
        # if td['cpu_threads'] > 1 and 'OMP_NUM_THREADS' not in task_env:
        #     cmd_options += ' -e OMP_NUM_THREADS=%(cpu_threads)s' % td

        task_args_str = self._create_arg_string(task_args)
        if task_args_str:
            task_exec += ' %s' % task_args_str

        cmd = '%s %s %s' % (self.launch_command, cmd_options, task_exec)
        self._log.debug('aprun cmd: %s', cmd)

        return cmd.strip(), None

# ------------------------------------------------------------------------------
