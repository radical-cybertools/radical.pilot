# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.base import LaunchMethod


# ------------------------------------------------------------------------------
#
class TestBaseLaunchMethod(TestCase):

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
        self.assertEqual(version, '')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_HYDRA)

        mocked_sh_callout.return_value = [
            'HYDRA build details:\n\n'
            'Version: 3.2\n\n'
            'Release Date: unreleased development copy\n\n'
            '/var/tmp/Intel-mvapich2/OFEDRPMS/BUILD/mvapich2\n\n'
            '2.3b-10/src/openpa/src', '', 0]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '')
        self.assertEqual(flavor, LaunchMethod.MPI_FLAVOR_HYDRA)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestBaseLaunchMethod()
    tc.test_get_mpi_info()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
