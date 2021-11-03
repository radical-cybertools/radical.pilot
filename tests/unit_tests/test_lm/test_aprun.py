# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.aprun import APRun


# ------------------------------------------------------------------------------
#
class TestAPRun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/aprun')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_aprun = APRun('', {}, None, None, None)

        lm_info = lm_aprun._init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_aprun = APRun('', {}, None, None, None)

        lm_info = {'env'        : {'test_env': 'test_value'},
                   'env_sh'     : 'env/lm_aprun.sh',
                   'command'    : '/usr/bin/aprun',
                   'mpi_version': '1.1.1',
                   'mpi_flavor' : APRun.MPI_FLAVOR_UNKNOWN}
        lm_aprun._init_from_info(lm_info)
        self.assertEqual(lm_aprun._env,         lm_info['env'])
        self.assertEqual(lm_aprun._env_sh,      lm_info['env_sh'])
        self.assertEqual(lm_aprun._command,     lm_info['command'])
        self.assertEqual(lm_aprun._mpi_version, lm_info['mpi_version'])
        self.assertEqual(lm_aprun._mpi_flavor,  lm_info['mpi_flavor'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_aprun._init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_aprun = APRun('', {}, None, None, None)
        self.assertTrue(lm_aprun.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_aprun.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_aprun = APRun('', {}, None, None, None)
        lm_info = {'env'        : {'test_env': 'test_value'},
                   'env_sh'     : 'env/lm_aprun.sh',
                   'command'    : '/usr/bin/aprun',
                   'mpi_version': '1.1.1',
                   'mpi_flavor' : APRun.MPI_FLAVOR_UNKNOWN}
        lm_aprun._init_from_info(lm_info)

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'],
                      lm_aprun.get_launcher_env())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds(self, mocked_logger, mocked_init):

        lm_aprun = APRun('', {}, None, None, None)
        lm_aprun._log     = mocked_logger
        lm_aprun._command = 'aprun'

        test_cases = setUp('lm', 'aprun')
        for task, result in test_cases:

            command = lm_aprun.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_aprun.get_rank_exec(task, None, None)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestAPRun()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
