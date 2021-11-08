# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.fork import Fork


# ------------------------------------------------------------------------------
#
class TestFork(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__', return_value=None)
    def test_init_from_scratch(self, mocked_init):

        lm_fork = Fork('', {}, None, None, None)

        env    = {'test_env': 'test_value'}
        env_sh = 'env/lm_fork.sh'

        lm_info = lm_fork._init_from_scratch(env, env_sh)
        self.assertEqual(lm_info, {'env'   : env,
                                   'env_sh': env_sh})

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_fork = Fork('', {}, None, None, None)

        lm_info = {'env'   : {'test_env': 'test_value'},
                   'env_sh': 'env/lm_fork.sh'}
        lm_fork._init_from_info(lm_info)
        self.assertEqual(lm_fork._env,    lm_info['env'])
        self.assertEqual(lm_fork._env_sh, lm_info['env_sh'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        # ensure single rank
        # (NOTE: full task and rank descriptions are NOT provided)

        lm_fork = Fork('', {}, None, None, None)
        lm_fork.node_name = 'local_machine'
        self.assertFalse(lm_fork.can_launch(
            task={'slots': {'ranks': [{'node_id': '00001'},
                                      {'node_id': '00002'}]}})[0])
        self.assertFalse(lm_fork.can_launch(
            task={'slots': {'ranks': [{'node_name': 'not_localhost_00'}]}})[0])
        self.assertFalse(lm_fork.can_launch(
            task={'description': {'executable': None},
                  'slots': {'ranks': [{'node_name': 'localhost'}]}})[0])
        self.assertTrue(lm_fork.can_launch(
            task={'description': {'executable': 'script'},
                  'slots': {'ranks': [{'node_name': 'localhost'}]}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_fork = Fork('', {}, None, None, None)
        lm_info = {'env'   : {'test_env': 'test_value'},
                   'env_sh': 'env/lm_fork.sh'}
        lm_fork._init_from_info(lm_info)
        lm_env = lm_fork.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__', return_value=None)
    def test_get_rank_exec(self, mocked_init):

        lm_fork = Fork('', {}, None, None, None)

        test_cases = setUp('lm', 'fork')
        for task, result in test_cases:
            command = lm_fork.get_rank_exec(task, None, None)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestFork()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_rank_exec()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
