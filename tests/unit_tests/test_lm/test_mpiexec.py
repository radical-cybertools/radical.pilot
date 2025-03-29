#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

import radical.utils as ru

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.mpiexec import MPIExec

MPI_INFO = ['1.1.1', 'OMPI']


# ------------------------------------------------------------------------------
#
class TestMPIExec(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info', return_value=MPI_INFO)
    @mock.patch('radical.utils.which', return_value='/bin/mpiexec')
    @mock.patch('radical.utils.get_hostname', return_value='localhost')
    @mock.patch('radical.utils.sh_callout', return_value=['-rf <msg>'])
    def test_init_from_scratch(self, mocked_sh_callout, mocked_hostname,
                               mocked_which, mocked_mpi_info, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec.name = 'mpiexec'
        lm_mpiexec._log = mock.Mock()
        lm_mpiexec._rm_info = ru.Config({'details': dict()})

        env    = {'test_env': 'test_value'}
        env_sh = 'env/lm_%s.sh' % lm_mpiexec.name.lower()

        lm_info = lm_mpiexec.init_from_scratch(env, env_sh)
        self.assertEqual(lm_info['env'],     env)
        self.assertEqual(lm_info['env_sh'],  env_sh)
        self.assertEqual(lm_info['command'], mocked_which())
        self.assertFalse(lm_info['mpt'])
        self.assertFalse(lm_info['rsh'])
        self.assertTrue(lm_info['use_rf'])
        self.assertFalse(lm_info['ccmrun'])
        self.assertFalse(lm_info['dplace'])
        self.assertFalse(lm_info['omplace'])
        self.assertEqual(lm_info['mpi_version'], mocked_mpi_info()[0])
        self.assertEqual(lm_info['mpi_flavor'],  mocked_mpi_info()[1])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info', return_value=MPI_INFO)
    @mock.patch('radical.utils.which', return_value='/bin/mpiexec')
    @mock.patch('radical.utils.get_hostname', return_value='localhost')
    @mock.patch('radical.utils.sh_callout', return_value=[''])
    def test_init_from_scratch_with_name(self, mocked_sh_callout,
                                         mocked_hostname, mocked_which,
                                         mocked_mpi_info, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec._log = mock.Mock()
        lm_mpiexec._rm_info = ru.Config({'details': dict()})

        for _flag in ['mpt', 'rsh']:
            lm_mpiexec.name = 'mpiexec_%s' % _flag
            lm_info = lm_mpiexec.init_from_scratch({}, '')
            self.assertTrue(lm_info[_flag])

        for _flavor in ['ccmrun', 'dplace']:
            lm_mpiexec.name = 'mpiexec_%s' % _flavor
            mocked_which.return_value = '/usr/bin/%s' % _flavor
            lm_info = lm_mpiexec.init_from_scratch({}, '')
            self.assertEqual(lm_info[_flavor], mocked_which())
            with self.assertRaises(ValueError):
                mocked_which.return_value = ''
                lm_mpiexec.init_from_scratch({}, '')

        lm_mpiexec.name = 'mpiexec'
        mocked_which.return_value = '/usr/bin/%s' % _flavor
        mocked_hostname.return_value = 'cheyenne'
        lm_info = lm_mpiexec.init_from_scratch({}, '')
        self.assertEqual(lm_info['omplace'], 'omplace')
        self.assertTrue(lm_info['mpt'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_mpiexec = MPIExec('', {}, {}, None, None)

        lm_info = {
            'env'        : {'test_env': 'test_value'},
            'env_sh'     : 'env/lm_mpirun.sh',
            'command'    : '/bin/mpiexec',
            'mpt'        : True,
            'rsh'        : False,
            'use_rf'     : True,
            'use_hf'     : False,
            'can_os'     : False,
            'ccmrun'     : '/bin/ccmrun',
            'dplace'     : '/bin/dplace',
            'omplace'    : '/bin/omplace',
            'mpi_version': '1.1.1',
            'mpi_flavor' : 'OMPI'
        }
        lm_mpiexec.init_from_info(lm_info)
        self.assertEqual(lm_mpiexec._env,         lm_info['env'])
        self.assertEqual(lm_mpiexec._env_sh,      lm_info['env_sh'])
        self.assertEqual(lm_mpiexec._command,     lm_info['command'])
        self.assertEqual(lm_mpiexec._mpt,         lm_info['mpt'])
        self.assertEqual(lm_mpiexec._rsh,         lm_info['rsh'])
        self.assertEqual(lm_mpiexec._use_rf,      lm_info['use_rf'])
        self.assertEqual(lm_mpiexec._use_hf,      lm_info['use_hf'])
        self.assertEqual(lm_mpiexec._can_os,      lm_info['can_os'])
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
        lm_mpiexec._mpt    = False

        lm_env = lm_mpiexec.get_launcher_env()
        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_mpiexec._env_sh, lm_env)
        self.assertNotIn('export MPI_SHEPHERD=true', lm_env)

        # special case - MPT
        lm_mpiexec._mpt = True

        lm_env = lm_mpiexec.get_launcher_env()
        self.assertIn('export MPI_SHEPHERD=true', lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_host_file(self, mocked_init):

        uid     = 'test_task.hf.0000'
        sandbox = './'
        slots   = [{'node_name' : 'node_A',
                    'node_index': 1,
                    'cores'     : [{'index': 1, 'occupation': 1.0},
                                   {'index': 2, 'occupation': 1.0},
                                   {'index': 4, 'occupation': 1.0},
                                   {'index': 5, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_A',
                    'node_index': 1,
                    'cores'     : [{'index': 6, 'occupation': 1.0},
                                   {'index': 7, 'occupation': 1.0},
                                   {'index': 8, 'occupation': 1.0},
                                   {'index': 9, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_B',
                    'node_index': 2,
                    'cores'     : [{'index': 0, 'occupation': 1.0},
                                   {'index': 1, 'occupation': 1.0},
                                   {'index': 2, 'occupation': 1.0},
                                   {'index': 3, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0}
        ]

        try:
            lm_mpiexec = MPIExec('', {}, None, None, None)

            host_file_expected = '%s/%s.hf' % (sandbox, uid)
            self.assertFalse(os.path.isfile(host_file_expected))

            host_file = lm_mpiexec._get_host_file(slots, uid, sandbox)
            self.assertEqual(host_file_expected, host_file)
            self.assertTrue(os.path.isfile(host_file))

            # simple host file
            with ru.ru_open(host_file) as hfd:
                hfd_content = hfd.read()
            self.assertEqual(hfd_content, 'node_A\nnode_B\n')

            # host file with "slots=" as delimiter for ranks
            lm_mpiexec._get_host_file(slots, uid, sandbox, mode=1)
            with ru.ru_open(host_file) as hfd:
                hfd_content = hfd.read()
            self.assertEqual(hfd_content, 'node_A slots=2\nnode_B slots=1\n')

            # host file with ":" as delimiter for ranks
            lm_mpiexec._get_host_file(slots, uid, sandbox, mode=2)
            with ru.ru_open(host_file) as hfd:
                hfd_content = hfd.read()
            self.assertEqual(hfd_content, 'node_A:2\nnode_B:1\n')

        finally:
            if os.path.isfile(host_file_expected):
                os.unlink(host_file)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_rank_file(self, mocked_init):

        uid     = 'test_task.rf.0001'
        sandbox = './'
        slots   = [{'node_name' : 'node_A',
                    'node_index': 1,
                    'cores'     : [{'index': 0, 'occupation': 1.0},
                                   {'index': 1, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_A',
                    'node_index': 1,
                    'cores'     : [{'index': 2, 'occupation': 1.0},
                                   {'index': 3, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_A',
                    'node_index': 1,
                    'cores'     : [{'index': 4, 'occupation': 1.0},
                                   {'index': 5, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_A',
                    'node_index': 1,
                    'cores'     : [{'index': 6, 'occupation': 1.0},
                                   {'index': 7, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_B',
                    'node_index': 2,
                    'cores'     : [{'index': 0, 'occupation': 1.0},
                                   {'index': 1, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0},
                   {'node_name' : 'node_B',
                    'node_index': 2,
                    'cores'     : [{'index': 2, 'occupation': 1.0},
                                   {'index': 3, 'occupation': 1.0}],
                    'gpus'      : [],
                    'lfs'       : 0,
                    'mem'       : 0}
        ]

        lm_mpiexec = MPIExec('', {}, None, None, None)

        rank_file_expected = '%s/%s.rf' % (sandbox, uid)
        self.assertFalse(os.path.isfile(rank_file_expected))

        rank_file = lm_mpiexec._get_rank_file(slots, uid, sandbox)
        self.assertEqual(rank_file_expected, rank_file)
        self.assertTrue(os.path.isfile(rank_file))

        with ru.ru_open(rank_file) as rfd:
            rfd_content = rfd.read()
        self.assertEqual(rfd_content,
                         'rank 0=node_A slots=0,1\n'
                         'rank 1=node_A slots=2,3\n'
                         'rank 2=node_A slots=4,5\n'
                         'rank 3=node_A slots=6,7\n'
                         'rank 4=node_B slots=0,1\n'
                         'rank 5=node_B slots=2,3\n')

        os.unlink(rank_file)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds(self, mocked_logger, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec.name     = 'mpiexec'
        lm_mpiexec._command = 'mpiexec'
        lm_mpiexec._mpt     = False
        lm_mpiexec._use_rf  = False
        lm_mpiexec._use_hf  = False
        lm_mpiexec._omplace = ''
        lm_mpiexec._log     = mocked_logger
        lm_mpiexec._rm_info = ru.Config({'details': dict()})

        test_cases = setUp('lm', 'mpiexec')
        for test_case in test_cases:

            task   = test_case[0]
            result = test_case[1]

            lm_mpiexec._mpi_flavor = task.get(
                'mpi_flavor', lm_mpiexec.MPI_FLAVOR_UNKNOWN)

            command = lm_mpiexec.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_mpiexec.get_exec(task)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

            if len(test_case) > 2:
                f_layout = test_case[2]
                f_name   = test_case[3]
                with ru.ru_open(f_name) as fd:
                    self.assertEqual(fd.readlines(), f_layout)
              # os.unlink(f_name)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds_mpt(self, mocked_logger, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec.name        = 'mpiexec_mpt'
        lm_mpiexec._command    = 'mpiexec_mpt'
        lm_mpiexec._mpt        = True
        lm_mpiexec._use_rf     = False
        lm_mpiexec._use_hf     = False
        lm_mpiexec._omplace    = 'omplace'
        lm_mpiexec._mpi_flavor = lm_mpiexec.MPI_FLAVOR_OMPI
        lm_mpiexec._log        = mocked_logger
        lm_mpiexec._rm_info    = ru.Config({'details': dict()})

        test_cases = setUp('lm', 'mpiexec_mpt')
        for task, result in test_cases:

            command = lm_mpiexec.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_mpiexec.get_exec(task)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__', return_value=None)
    def test_get_rank_cmd(self, mocked_init):

        lm_mpiexec = MPIExec('', {}, None, None, None)
        lm_mpiexec._mpt = False
        lm_mpiexec._mpi_flavor = lm_mpiexec.MPI_FLAVOR_OMPI

        command = lm_mpiexec.get_rank_cmd()
        self.assertIn('$MPI_RANK',  command)
        self.assertIn('$PMIX_RANK', command)

        self.assertNotIn('$PMI_ID', command)
        lm_mpiexec._mpi_flavor = lm_mpiexec.MPI_FLAVOR_HYDRA
        command = lm_mpiexec.get_rank_cmd()
        self.assertIn('$PMI_ID',    command)
        self.assertIn('$PMI_RANK',  command)

        self.assertNotIn('$PALS_RANKID', command)
        lm_mpiexec._mpi_flavor = lm_mpiexec.MPI_FLAVOR_PALS
        command = lm_mpiexec.get_rank_cmd()
        self.assertIn('$PALS_RANKID', command)

        # special case - MPT
        self.assertNotIn('$MPT_MPI_RANK', command)
        lm_mpiexec._mpt = True
        command = lm_mpiexec.get_rank_cmd()
        self.assertIn('$MPT_MPI_RANK', command)

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
    tc.test_get_rank_cmd()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
