# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.pilot.constants as rpc
from radical.pilot.agent.resource_manager.base import RMInfo

from .test_common import setUp
from radical.pilot.agent.launch_method.ibrun import IBRun


class TestIBRun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/ibrun')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)

        lm_info = lm_ibrun._init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_ibrun = IBRun('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ibrun.sh',
                   'command': '/usr/bin/ibrun'}
        lm_ibrun._init_from_info(lm_info)
        self.assertEqual(lm_ibrun._env,     lm_info['env'])
        self.assertEqual(lm_ibrun._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_ibrun._command, lm_info['command'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_ibrun._init_from_info(lm_info)

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
        lm_ibrun._init_from_info(lm_info)
        lm_env = lm_ibrun.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__',   return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        cores_per_node = 16
        gpus_per_node  = 1

        lm_ibrun = IBRun('', {}, None, None, None)
        lm_ibrun._command = 'ibrun'
        lm_ibrun._rm_info = RMInfo({
            'cores_per_node': cores_per_node,
            'gpus_per_node' : gpus_per_node,
            'node_list': [
                {'node_name': 'node1',
                 'node_id'  : 'node1',
                 'cores'    : [rpc.FREE] * cores_per_node,
                 'gpus'     : [rpc.FREE] * gpus_per_node,
                 'lfs'      : 0,
                 'mem'      : 0},
                {'node_name': 'node2',
                 'node_id'  : 'node2',
                 'cores'    : [rpc.FREE] * cores_per_node,
                 'gpus'     : [rpc.FREE] * gpus_per_node,
                 'lfs'      : 0,
                 'mem'      : 0}
            ]
        })

        test_cases = setUp('lm', 'ibrun')
        for task, result in test_cases:

            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    lm_ibrun.get_launch_cmds(task, '')

            elif result == 'AssertionError':
                with self.assertRaises(AssertionError):
                    lm_ibrun.get_launch_cmds(task, '')

            else:
                command = lm_ibrun.get_launch_cmds(task, '')
                self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

                command = lm_ibrun.get_rank_exec(task, None, None)
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
