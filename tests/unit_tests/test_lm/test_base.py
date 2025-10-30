# pylint: disable=protected-access,unused-argument,no-value-for-parameter,abstract-method

import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.base import LaunchMethod

os.environ['RADICAL_BASE'] = '/tmp/rp_tests_%s' % os.getuid()


# ------------------------------------------------------------------------------
#
class TestBaseLaunchMethod(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    @mock.patch('radical.utils.env_eval')
    def test_init_from_registry(self, mocked_env_eval, mocked_prof, mocked_log):

        class NewLaunchMethod(LaunchMethod):

            def init_from_info(self, lm_info):
                self._env     = lm_info['env']
                self._env_sh  = lm_info['env_sh']
                self._command = lm_info['command']
                assert self._command

        # check initialization from registry data only,

        ru.rec_makedir('/tmp/rp_tests_%s' % os.getuid())
        reg = ru.zmq.Registry(path='/tmp/rp_tests_%s' % os.getuid())
        reg.start()

        lm_name = 'NewLaunchMethod'
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_new.sh',
                   'command': '/usr/bin/test'}

        c = ru.zmq.RegistryClient(url=reg.addr)
        c.put('lm.%s' % lm_name.lower(), lm_info)
        c.close()

        lm = NewLaunchMethod(lm_name, ru.TypedDict({'reg_addr': reg.addr}), None,
                             mock.Mock(), mock.Mock())

        self.assertEqual(lm._env,     lm_info['env'])
        self.assertEqual(lm._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm._command, lm_info['command'])

        reg.stop()
        reg.wait()

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LaunchMethod, '__init__', return_value=None)
    @mock.patch('radical.utils.sh_callout')
    @mock.patch('radical.utils.Logger')
    def test_get_mpi_info(self, mocked_logger, mocked_sh_callout, mocked_init):

        lm = LaunchMethod('', {}, None, None, None)
        lm._log = mocked_logger

        with self.assertRaises(ValueError):
            # no executable found
            lm._get_mpi_info(exe='')

        with self.assertRaises(RuntimeError):
            mocked_sh_callout.return_value = ['', '', 1]
            lm._get_mpi_info('mpirun')

        mocked_sh_callout.return_value = ['19.05.2', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertIsNone(version)  # correct version is not set
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_UNKNOWN)

        mocked_sh_callout.return_value = [
            'mpirun (Open MPI) 2.1.2\n\n'
            'Report bugs to https://www.open-mpi.org/community/help/\n', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '2.1.2')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_OMPI)

        mocked_sh_callout.return_value = ['HYDRA build details:', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_HYDRA)

        mocked_sh_callout.return_value = [
            'Intel(R) MPI Library for Linux* OS,\n\n'
            'Version 2019 Update 5 Build 20190806\n\n'
            'Copyright 2003-2019, Intel Corporation.', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '2019 update 5 build 20190806')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_HYDRA)

        mocked_sh_callout.return_value = [
            'HYDRA build details:\n\n'
            'Version: 3.2\n\n'
            'Release Date: unreleased development copy\n\n'
            '/var/tmp/Intel-mvapich2/OFEDRPMS/BUILD/mvapich2\n\n'
            '2.3b-10/src/openpa/src', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '3.2')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_HYDRA)

        mocked_sh_callout.return_value = ['Version: 1.1', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '1.1')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_OMPI)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestBaseLaunchMethod()
    tc.test_get_mpi_info()


# ------------------------------------------------------------------------------
