
# pylint: disable=protected-access, unused-argument


from   test_common                   import setUp
from   radical.pilot.agent.lm.mpirun import MPIRun

import radical.utils as ru


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIRun, '__init__',   return_value=None)
@mock.patch.object(MPIRun, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'mpirun')
    component  = MPIRun(name=None, cfg=None, session=None)

    component._log           = ru.get_logger('dummy')
    component.name           = 'MPIRun'
    component.mpi_flavor     = None
    component.launch_command = 'mpirun'
    component.ccmrun_command = ''
    component.dplace_command = ''

    for unit, result in test_cases:
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)


# ------------------------------------------------------------------------------



