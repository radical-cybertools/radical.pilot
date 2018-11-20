import radical.utils as ru
from radical.pilot.agent.lm.yarn import Yarn
import json

try:
    import mock
except ImportError:
    from unittest import mock

# pylint: disable=protected-access, unused-argument


# Setup to be done for every test
# -----------------------------------------------------------------------------
def setUp():

    test_cases = json.load(open('tests/lm/unit/test_cases_yarn.json'))

    return test_cases
# -----------------------------------------------------------------------------


def tearDown():
    pass
# -----------------------------------------------------------------------------


# Test YARN construct_command method
# -----------------------------------------------------------------------------
@mock.patch.object(Yarn, '__init__', return_value=None)
@mock.patch.object(Yarn, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure,
                           mocked_raise_on):

    test_cases = setUp()

    component = Yarn(cfg=None, session=None, name=None)
    component._log  = ru.get_logger('dummy')
    component.launch_command = 'yarn'
    for i in range(len(test_cases['trigger'])):
        cu = test_cases['trigger'][i]
        command,_ = component.construct_command(cu,None)
        print command
        assert command == test_cases['result'][i]

    tearDown()
