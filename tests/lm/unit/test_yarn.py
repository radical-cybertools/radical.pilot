
# pylint: disable=protected-access, unused-argument

from   test_common                 import setUp
from   radical.pilot.agent.lm.yarn import Yarn

import radical.utils as ru

import pytest

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Yarn, '__init__',   return_value=None)
@mock.patch.object(Yarn, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'yarn')
    component  = Yarn(cfg=None, session=None, name=None)

    component._log           = ru.get_logger('dummy')
    component.launch_command = 'yarn'
    component.name           = "YARN"

    for unit, result in test_cases:
        if result == "RuntimeError":
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(unit, None)
        else:
            command, hop = component.construct_command(unit, None)
            assert([command, hop] == result)
        

# ------------------------------------------------------------------------------

