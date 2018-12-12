
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
@mock.patch.object(MPIRun, '_get_mpi_info', return_value=[5,'ORTE'])
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/mpirun')
def test_configure(mocked_init, mocked_get_mpi_info, mocked_raise_on,
                   mocked_which):

    component = MPIRun(name=None, cfg=None, session=None)
    component.name = 'MPIRun'
    component._configure()
    assert('mpirun' == component.launch_command)
    assert(5 == component.mpi_version)
    assert('ORTE' == component.mpi_flavor)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIRun, '__init__',   return_value=None)
@mock.patch.object(MPIRun, '_get_mpi_info', return_value=[5,'ORTE'])
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/mpirun')
def test_configure_rsh(mocked_init, mocked_get_mpi_info, mocked_raise_on,
                   mocked_which):

    component = MPIRun(name=None, cfg=None, session=None)
    component.name = 'MPIRun_rsh'
    component._configure()
    assert('mpirun' == component.launch_command)
    assert(5 == component.mpi_version)
    assert('ORTE' == component.mpi_flavor)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIRun, '__init__',   return_value=None)
@mock.patch.object(MPIRun, '_get_mpi_info', return_value=[5,'ORTE'])
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/mpirun')
def test_configure_mpt(mocked_init, mocked_get_mpi_info, mocked_raise_on,
                   mocked_which):

    component = MPIRun(name=None, cfg=None, session=None)
    component.name = 'MPIRun_mpt'
    component._configure()
    assert('mpirun' == component.launch_command)
    assert(5 == component.mpi_version)
    assert('ORTE' == component.mpi_flavor)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIRun, '__init__',   return_value=None)
@mock.patch.object(MPIRun, '_get_mpi_info', return_value=[5,'ORTE'])
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/mpirun')
def test_configure_ccmrun(mocked_init, mocked_get_mpi_info, mocked_raise_on,
                   mocked_which):

    component = MPIRun(name=None, cfg=None, session=None)
    component.name = 'MPIRun_ccmrun'
    component._configure()
    assert('mpirun' == component.launch_command)
    assert(5 == component.mpi_version)
    assert('ORTE' == component.mpi_flavor)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(MPIRun, '__init__',   return_value=None)
@mock.patch.object(MPIRun, '_get_mpi_info', return_value=[5,'ORTE'])
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/mpirun')
def test_configure_dplace(mocked_init, mocked_get_mpi_info, mocked_raise_on,
                   mocked_which):

    component = MPIRun(name=None, cfg=None, session=None)
    component.name = 'MPIRun_dplace'
    component._configure()
    assert('mpirun' == component.launch_command)
    assert(5 == component.mpi_version)
    assert('ORTE' == component.mpi_flavor)

# ------------------------------------------------------------------------------


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



