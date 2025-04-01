#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.mpirun import MPIRun


# ------------------------------------------------------------------------------
#
class TestMPIRun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__', return_value=None)
    @mock.patch.object(MPIRun, '_get_mpi_info', return_value=['1.1.1', 'ORTE'])
    @mock.patch('radical.utils.which', return_value='/bin/mpirun')
    @mock.patch('radical.utils.get_hostname', return_value='localhost')
    def test_init_from_scratch(self, mocked_hostname, mocked_which,
                               mocked_mpi_info, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)
        lm_mpirun.name = 'mpirun'
        lm_mpirun._log = mock.Mock()

        env    = {'test_env': 'test_value'}
        env_sh = 'env/lm_%s.sh' % lm_mpirun.name.lower()

        lm_info = lm_mpirun.init_from_scratch(env, env_sh)
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
    @mock.patch.object(MPIRun, '__init__', return_value=None)
    @mock.patch.object(MPIRun, '_get_mpi_info', return_value=['1.1.1', 'ORTE'])
    @mock.patch('radical.utils.which', return_value='/bin/mpirun')
    @mock.patch('radical.utils.get_hostname', return_value='localhost')
    def test_init_from_scratch_with_name(self, mocked_hostname, mocked_which,
                                         mocked_mpi_info, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)
        lm_mpirun._log = mock.Mock()

        for _flag in ['mpt', 'rsh']:
            lm_mpirun.name = 'mpirun_%s' % _flag
            lm_info = lm_mpirun.init_from_scratch({}, '')
            self.assertTrue(lm_info[_flag])

        for _flavor in ['ccmrun', 'dplace']:
            lm_mpirun.name = 'mpirun_%s' % _flavor
            mocked_which.return_value = '/usr/bin/%s' % _flavor
            lm_info = lm_mpirun.init_from_scratch({}, '')
            self.assertEqual(lm_info[_flavor], mocked_which())
            with self.assertRaises(ValueError):
                mocked_which.return_value = ''
                lm_mpirun.init_from_scratch({}, '')

        lm_mpirun.name = 'mpirun'
        mocked_hostname.return_value = 'cheyenne'
        mocked_which.return_value = '/usr/bin/omplace'
        lm_info = lm_mpirun.init_from_scratch({}, '')
        self.assertEqual(lm_info['omplace'], mocked_which())
        self.assertTrue(lm_info['mpt'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)

        lm_info = {
            'env'        : {'test_env': 'test_value'},
            'env_sh'     : 'env/lm_mpirun.sh',
            'command'    : '/bin/mpirun',
            'mpt'        : True,
            'rsh'        : False,
            'ccmrun'     : '/bin/ccmrun',
            'dplace'     : '/bin/dplace',
            'omplace'    : '/bin/omplace',
            'mpi_version': '1.1.1',
            'mpi_flavor' : 'ORTE'
        }
        lm_mpirun.init_from_info(lm_info)
        self.assertEqual(lm_mpirun._env,         lm_info['env'])
        self.assertEqual(lm_mpirun._env_sh,      lm_info['env_sh'])
        self.assertEqual(lm_mpirun._command,     lm_info['command'])
        self.assertEqual(lm_mpirun._mpt,         lm_info['mpt'])
        self.assertEqual(lm_mpirun._rsh,         lm_info['rsh'])
        self.assertEqual(lm_mpirun._ccmrun,      lm_info['ccmrun'])
        self.assertEqual(lm_mpirun._dplace,      lm_info['dplace'])
        self.assertEqual(lm_mpirun._omplace,     lm_info['omplace'])
        self.assertEqual(lm_mpirun._mpi_version, lm_info['mpi_version'])
        self.assertEqual(lm_mpirun._mpi_flavor,  lm_info['mpi_flavor'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)
        self.assertTrue(lm_mpirun.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_mpirun.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)
        lm_mpirun._env_sh = 'env/lm_mpirun.sh'
        lm_mpirun._mpt    = False

        lm_env = lm_mpirun.get_launcher_env()
        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_mpirun._env_sh, lm_env)
        self.assertNotIn('export MPI_SHEPHERD=true', lm_env)

        # special case - MPT
        lm_mpirun._mpt = True

        lm_env = lm_mpirun.get_launcher_env()
        self.assertIn('export MPI_SHEPHERD=true', lm_env)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__',   return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)
        lm_mpirun.name     = 'mpirun'
        lm_mpirun._command = 'mpirun'
        lm_mpirun._mpt     = False
        lm_mpirun._rsh     = False
        lm_mpirun._ccmrun  = ''
        lm_mpirun._dplace  = ''
        lm_mpirun._omplace = ''

        test_cases = setUp('lm', 'mpirun')
        for task, result in test_cases:

            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    lm_mpirun.get_launch_cmds(task, '')
                continue

            lm_mpirun._mpi_flavor = task.get('mpi_flavor', 'unknown')

            command = lm_mpirun.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_mpirun.get_exec(task)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIRun, '__init__', return_value=None)
    def test_get_rank_cmd(self, mocked_init):

        lm_mpirun = MPIRun('', {}, None, None, None)
        lm_mpirun._mpt        = False
        lm_mpirun._mpi_flavor = lm_mpirun.MPI_FLAVOR_OMPI

        command = lm_mpirun.get_rank_cmd()
        self.assertIn('$MPI_RANK', command)
        self.assertIn('$PMIX_RANK', command)

        self.assertNotIn('$PMI_ID', command)
        lm_mpirun._mpi_flavor = lm_mpirun.MPI_FLAVOR_HYDRA
        command = lm_mpirun.get_rank_cmd()
        self.assertIn('$PMI_ID', command)
        self.assertIn('$PMI_RANK', command)

        self.assertNotIn('$PALS_RANKID', command)
        lm_mpirun._mpi_flavor = lm_mpirun.MPI_FLAVOR_PALS
        command = lm_mpirun.get_rank_cmd()
        self.assertIn('$PALS_RANKID', command)

        # special case - MPT
        self.assertNotIn('$MPT_MPI_RANK', command)
        lm_mpirun._mpt = True
        command = lm_mpirun.get_rank_cmd()
        self.assertIn('$MPT_MPI_RANK', command)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestMPIRun()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_with_name()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()
    tc.test_get_rank_cmd()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
