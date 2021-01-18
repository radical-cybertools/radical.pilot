
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import pytest

from unittest import mock

import radical.utils as ru

from .test_common                           import setUp
from radical.pilot.agent.launch_method.prte import PRTE


# ------------------------------------------------------------------------------
#
@mock.patch.object(PRTE, '__init__', return_value=None)
@mock.patch.object(PRTE, '_configure', return_value='prun')
def test_construct_command(mocked_init, mocked_configure):

    test_cases = setUp('lm', 'prte')

    component = PRTE(name=None, cfg=None, session=None)

    component.name           = 'prte'
    component._verbose       = None
    component._log           = ru.Logger('dummy')
    component.launch_command = 'prun'

    for task, result in test_cases:

        if result == "RuntimeError":
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(task, None)

        else:
            command, hop = component.construct_command(task, None)
            assert([command, hop] == result), task['uid']


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
