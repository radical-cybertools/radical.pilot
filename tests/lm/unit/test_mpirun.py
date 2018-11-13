import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.mpirun import MPIRun
import json
import os
try:
    import mock
except ImportError:
    from unittest import mock

# pylint: disable=protected-access, unused-argument

# Setup to be done for every test
# -----------------------------------------------------------------------------


def setUp():

    curdir = os.path.dirname(os.path.abspath(__file__))
    test_cases = json.load(open('%s/test_cases_mpirun.json' % curdir))

    return test_cases
# -----------------------------------------------------------------------------


def tearDown():

    pass

# -----------------------------------------------------------------------------


# Test Summit Scheduler construct_command method
# -----------------------------------------------------------------------------
@mock.patch.object(MPIRun, '__init__', return_value=None)
@mock.patch.object(MPIRun, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure,
                           mocked_raise_on):

    test_cases = setUp()

    component = MPIRun(cfg=None, session=None)
    component._log = ru.get_logger('dummy')
    component.launch_command = 'mpirun'
    component.mpi_flavor = None
    for i in range(len(test_cases['trigger'])):
        cu = test_cases['trigger'][i]
        command, _ = component.construct_command(cu, None)
        assert command == test_cases['result'][i]

    tearDown()
