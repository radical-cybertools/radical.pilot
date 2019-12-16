
# pylint: disable=protected-access, unused-argument

from   .test_common                    import setUp
from   radical.pilot.agent.lm.mpiexec import MPIExec

import radical.utils as ru


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIExec, '__init__',   return_value=None)
@mock.patch.object(MPIExec, '_get_mpi_info', return_value=[5,'ORTE'])
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='mpiexec')
def test_configure(mocked_init, mocked_get_mpi_info, mocked_raise_on,
                   mocked_which):

    component = MPIExec(name=None, cfg=None, session=None)
    component.name = 'MPIExec'
    component._configure()
    assert('mpiexec' == component.launch_command)
    assert(5 == component.mpi_version)
    assert('ORTE' == component.mpi_flavor)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIExec, '__init__',   return_value=None)
@mock.patch.object(MPIExec, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init,
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'mpiexec')
    component  = MPIExec(name=None, cfg=None, session=None)

    component._log           = ru.Logger('dummy')
    component.name           = 'MPIExec'
    component.mpi_flavor     = None
    component.launch_command = 'mpiexec'

    for unit, result in test_cases:
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)

# ------------------------------------------------------------------------------
