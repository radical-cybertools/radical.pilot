# pylint: disable=protected-access, unused-argument

import radical.utils as ru
from .test_common                  import setUp
from radical.pilot.agent.lm.prte import PRTE
import pytest

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(PRTE, '__init__', return_value=None)
@mock.patch.object(PRTE, '_configure', return_value='prun')
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure, mocked_raise_on):

    test_cases = setUp('lm', 'prte')

    component = PRTE(name=None, cfg=None, session=None)
    component.name = 'prte'
    component._verbose = None
    component._log  = ru.Logger('dummy')
    component.launch_command = 'prun'
    for unit, result in test_cases:
        if result == "RuntimeError":
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(unit, None)
        else:
            command, hop = component.construct_command(unit, None)
            assert([command, hop] == result)
# ------------------------------------------------------------------------------
