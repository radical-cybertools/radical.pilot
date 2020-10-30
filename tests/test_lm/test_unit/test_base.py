
# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase
from radical.pilot.agent.launch_method.base import LaunchMethod

import radical.utils as ru
import pytest

try:
    import mock
except ImportError:
    from unittest import mock


class TestBase(TestCase):

    def test_configure(self):

        session = mock.Mock()
        session._log = mock.Mock()
        with pytest.raises(NotImplementedError):
            LaunchMethod(name='test', cfg={}, session=session)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(LaunchMethod,'__init__',return_value=None)
    def test_get_mpi_info(self, mocked_init):

        lm = LaunchMethod(name=None, cfg={}, session=None)
        lm._log = mock.Mock()
        ru.sh_callout = mock.Mock()
        ru.sh_callout.side_effect = [['test',1,0]]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertIsNone(version)
        self.assertEqual(flavor, 'unknown')

        ru.sh_callout.side_effect = [['test',1,1],['mpirun (Open MPI) 2.1.2\n\n\
                                    Report bugs to http://www.open-mpi.org/community/help/\n',3,0]]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '2.1.2')
        self.assertEqual(flavor,'OMPI')

        ru.sh_callout.side_effect = [['test',1,1],['HYDRA build details:',3,0]]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '')
        self.assertEqual(flavor, 'HYDRA')

        ru.sh_callout.side_effect = [['test',1,1],['Intel(R) MPI Library for Linux* OS,\n\n\
                                      Version 2019 Update 5 Build 20190806\n\n\
                                      Copyright 2003-2019, Intel Corporation.',3,0]]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '')
        self.assertEqual(flavor, 'HYDRA')

        ru.sh_callout.side_effect = [['test',1,1],['HYDRA build details:\n\n\
                                    Version: 3.2\n\n\
                                    Release Date: unreleased development copy\n\n\
                                    /var/tmp/Intel-mvapich2/OFEDRPMS/BUILD/mvapich2\n\n\
                                    2.3b-10/src/openpa/src',3,0]]
        version, flavor = lm._get_mpi_info('mpirun')
        self.assertEqual(version, '')
        self.assertEqual(flavor,'HYDRA')
