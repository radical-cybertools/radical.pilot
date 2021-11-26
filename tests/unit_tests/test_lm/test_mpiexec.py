# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.mpiexec import MPIExec


# ------------------------------------------------------------------------------
#
class TestMPIExec(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info', return_value=['1.1.1', 'OMPI'])
    @mock.patch('radical.utils.which', return_value='/bin/mpiexec')
    @mock.patch('radical.utils.get_hostname', return_value='localhost')
    def test_init_from_scratch(self, mocked_hostname, mocked_which,
                               mocked_mpi_info, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec.name = 'mpiexec'

        env    = {'test_env': 'test_value'}
        env_sh = 'env/lm_%s.sh' % lm_mpiexec.name.lower()

        lm_info = lm_mpiexec._init_from_scratch(env, env_sh)
        self.assertEqual(lm_info['env'],     env)
        self.assertEqual(lm_info['env_sh'],  env_sh)
        self.assertEqual(lm_info['command'], mocked_which())
        self.assertFalse(lm_info['mpt'])
        self.assertFalse(lm_info['rsh'])
        self.assertFalse(lm_info['ccmrun'])
        self.assertFalse(lm_info['dplace'])
        self.assertFalse(lm_info['omplace'])
        self.assertEqual(lm_info['mpi_version'], mocked_mpi_info()[0])
        self.assertEqual(lm_info['mpi_flavor'],  mocked_mpi_info()[1])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info', return_value=['1.1.1', 'OMPI'])
    @mock.patch('radical.utils.which', return_value='/bin/mpiexec')
    @mock.patch('radical.utils.get_hostname', return_value='localhost')
    def test_init_from_scratch_with_name(self, mocked_hostname, mocked_which,
                                         mocked_mpi_info, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)

        for _flag in ['mpt', 'rsh']:
            lm_mpiexec.name = 'mpiexec_%s' % _flag
            lm_info = lm_mpiexec._init_from_scratch({}, '')
            self.assertTrue(lm_info[_flag])

        for _flavor in ['ccmrun', 'dplace']:
            lm_mpiexec.name = 'mpiexec_%s' % _flavor
            mocked_which.return_value = '/usr/bin/%s' % _flavor
            lm_info = lm_mpiexec._init_from_scratch({}, '')
            self.assertEqual(lm_info[_flavor], mocked_which())
            with self.assertRaises(AssertionError):
                mocked_which.return_value = ''
                lm_mpiexec._init_from_scratch({}, '')

        lm_mpiexec.name = 'mpiexec'
        mocked_hostname.return_value = 'cheyenne'
        lm_info = lm_mpiexec._init_from_scratch({}, '')
        self.assertEqual(lm_info['omplace'], 'omplace')
        self.assertTrue(lm_info['mpt'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)

        lm_info = {
            'env'        : {'test_env': 'test_value'},
            'env_sh'     : 'env/lm_mpirun.sh',
            'command'    : '/bin/mpiexec',
            'mpt'        : True,
            'rsh'        : False,
            'ccmrun'     : '/bin/ccmrun',
            'dplace'     : '/bin/dplace',
            'omplace'    : '/bin/omplace',
            'mpi_version': '1.1.1',
            'mpi_flavor' : 'OMPI'
        }
        lm_mpiexec._init_from_info(lm_info)
        self.assertEqual(lm_mpiexec._env,         lm_info['env'])
        self.assertEqual(lm_mpiexec._env_sh,      lm_info['env_sh'])
        self.assertEqual(lm_mpiexec._command,     lm_info['command'])
        self.assertEqual(lm_mpiexec._mpt,         lm_info['mpt'])
        self.assertEqual(lm_mpiexec._rsh,         lm_info['rsh'])
        self.assertEqual(lm_mpiexec._ccmrun,      lm_info['ccmrun'])
        self.assertEqual(lm_mpiexec._dplace,      lm_info['dplace'])
        self.assertEqual(lm_mpiexec._omplace,     'omplace')
        self.assertEqual(lm_mpiexec._mpi_version, lm_info['mpi_version'])
        self.assertEqual(lm_mpiexec._mpi_flavor,  lm_info['mpi_flavor'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        self.assertTrue(lm_mpiexec.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_mpiexec.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec._env_sh = 'env/lm_mpiexec.sh'

        lm_env = lm_mpiexec.get_launcher_env()
        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_mpiexec._env_sh, lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds(self, mocked_logger, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec.name     = 'mpiexec'
        lm_mpiexec._command = 'mpiexec'
        lm_mpiexec._mpt     = False
        lm_mpiexec._omplace = ''
        lm_mpiexec._log     = mocked_logger

        test_cases = setUp('lm', 'mpiexec')
        for task, result in test_cases:

            command = lm_mpiexec.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_mpiexec.get_rank_exec(task, None, None)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds_mpt(self, mocked_logger, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec.name     = 'mpiexec_mpt'
        lm_mpiexec._command = 'mpiexec_mpt'
        lm_mpiexec._mpt     = True
        lm_mpiexec._omplace = 'omplace'
        lm_mpiexec._log     = mocked_logger

        test_cases = setUp('lm', 'mpiexec_mpt')
        for task, result in test_cases:

            command = lm_mpiexec.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_mpiexec.get_rank_exec(task, None, None)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestMPIExec()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_with_name()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()
    tc.test_get_launch_rank_cmds_mpt()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
