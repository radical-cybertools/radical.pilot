
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from   .test_common                              import setUp
from   radical.pilot.agent.launch_method.mpiexec import MPIExec


class TestMPIExec(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info',
                       return_value=[5, MPIExec.MPI_FLAVOR_OMPI])
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value='mpiexec')
    def test_configure(self, mocked_init, mocked_get_mpi_info, mocked_raise_on,
                       mocked_which):

        component = MPIExec(name=None, cfg=None, session=None)
        component.name     = 'MPIEXEC'
        component._mpt     = False
        component._omplace = False

        component._configure()

        self.assertEqual('mpiexec', component.launch_command)
        self.assertEqual(5, component.mpi_version)
        self.assertEqual('OMPI', component.mpi_flavor)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info',
                       return_value=[5, MPIExec.MPI_FLAVOR_OMPI])
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value='mpiexec_mpt')
    def test_configure_mpt(self, mocked_init, mocked_get_mpi_info, mocked_raise_on,
                           mocked_which):

        component = MPIExec(name=None, cfg=None, session=None)
        component.name     = 'MPIEXEC_MPT'
        component._mpt     = False
        component._omplace = False

        component._configure()

        self.assertTrue(component._mpt)
        self.assertEqual('mpiexec_mpt', component.launch_command)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch.object(MPIExec, '_configure', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init,
                               mocked_configure,
                               mocked_raise_on):

        test_cases = setUp('lm', 'mpiexec')
        component  = MPIExec(name=None, cfg=None, session=None)
        component.name           = 'MPIEXEC'
        component._log           = ru.Logger('dummy')
        component._mpt           = False
        component._omplace       = False
        component.mpi_flavor     = 'unknown'
        component.launch_command = 'mpiexec'

        for task, result in test_cases:
            command, hop = component.construct_command(task, None)
            self.assertEqual([command, hop], result)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(MPIExec, '__init__',   return_value=None)
    @mock.patch.object(MPIExec, '_get_mpi_info',
                       return_value=[5, MPIExec.MPI_FLAVOR_OMPI])
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value='mpiexec_mpt')
    def test_construct_command_mpt(self, mocked_init,
                                   mocked_get_mpi_info,
                                   mocked_raise_on,
                                   mocked_which):

        test_cases = setUp('lm', 'mpiexec_mpt')
        component  = MPIExec(name=None, cfg=None, session=None)
        component.name           = 'MPIEXEC_MPT'
        component._log           = ru.Logger('dummy')
        component._mpt           = False
        component._omplace       = False

        component._configure()

        for task, result in test_cases:
            command, hop = component.construct_command(task, None)
            self.assertEqual([command, hop], result)
            self.assertEqual(task['description']['environment'].get('MPI_SHEPHERD'), 'true')


if __name__ == '__main__':

    tc = TestMPIExec()
    tc.test_configure()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
