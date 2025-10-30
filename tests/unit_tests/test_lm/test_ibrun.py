#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.pilot.constants as rpc
from radical.pilot.agent.resource_manager.base import RMInfo

from . import test_common as tc
from radical.pilot.agent.launch_method.ibrun import IBRun


class TestIBRun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/ibrun')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)

        lm_info = lm_ibrun.init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ibrun.sh',
                   'command': '/usr/bin/ibrun'}
        lm_ibrun.init_from_info(lm_info)
        self.assertEqual(lm_ibrun._env,     lm_info['env'])
        self.assertEqual(lm_ibrun._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_ibrun._command, lm_info['command'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_ibrun.init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)
        self.assertTrue(lm_ibrun.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_ibrun.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ibrun.sh',
                   'command': '/usr/bin/ibrun'}
        lm_ibrun.init_from_info(lm_info)
        lm_env = lm_ibrun.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__',   return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)
        lm_ibrun._command = 'ibrun'

        test_cases = tc.setUp('lm', 'ibrun')
        for task, result in test_cases:

            if result == 'AssertionError':
                with self.assertRaises(AssertionError):
                    lm_ibrun.get_launch_cmds(task, '')
                continue

            cores_per_node = task.get('cores_per_node', 1)
            gpus_per_node  = task.get('gpus_per_node',  0)

            lm_ibrun._rm_info = RMInfo({
                'cores_per_node': cores_per_node,
                'gpus_per_node' : gpus_per_node,
                'node_list'     : [
                    {'name'     : 'node1',
                     'index'    : 1,
                     'cores'    : [rpc.FREE] * cores_per_node,
                     'gpus'     : [rpc.FREE] * gpus_per_node,
                     'lfs'      : 0,
                     'mem'      : 0},
                    {'name'     : 'node2',
                     'index'    : 2,
                     'cores'    : [rpc.FREE] * cores_per_node,
                     'gpus'     : [rpc.FREE] * gpus_per_node,
                     'lfs'      : 0,
                     'mem'      : 0}
                ]
            })

            lm_ibrun._lm_cfg = task.get('lm_cfg', {})

            command = lm_ibrun.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_ibrun.get_exec(task)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    def test_get_rank_cmd(self, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)

        command = lm_ibrun.get_rank_cmd()
        self.assertIn('$SLURM_PROCID', command)
        self.assertIn('$MPI_RANK',     command)
        self.assertIn('$PMIX_RANK',    command)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestIBRun()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()
    tc.test_get_rank_cmd()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
