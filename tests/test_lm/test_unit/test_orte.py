
# pylint: disable=protected-access, unused-argument

from   test_common                 import setUp
from   radical.pilot.agent.lm.orte import ORTE

import radical.utils as ru

import pytest
try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(ORTE, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/orterun')
def test_configure(mocked_init, mocked_raise_on, mocked_which):

    component = ORTE(name=None, cfg=None, session=None)
    component._configure()
    assert('/usr/bin/orterun' == component.launch_command)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(ORTE, '__init__',   return_value=None)
@mock.patch.object(ORTE, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'orte')
    component  = ORTE(name=None, cfg=None, session=None)

    component.launch_command = 'orterun'
    component.name           = 'orte'
    component._log           = ru.Logger('dummy')

    for unit, result in test_cases:
        if result == "RuntimeError":
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(unit, None)
        else:
            command, hop = component.construct_command(unit, None)
            assert([command, hop] == result)


# ------------------------------------------------------------------------------

