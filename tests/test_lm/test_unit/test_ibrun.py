# pylint: disable=protected-access, unused-argument

import pytest
try:
    import mock
except ImportError:
    from unittest import mock

import radical.utils as ru
from radical.pilot.agent.launch_method.ibrun import IBRun

from .test_common import setUp


# ------------------------------------------------------------------------------
#
@mock.patch.object(IBRun, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/ibrun')
def test_configure(mocked_init, mocked_raise_on, mocked_which):

    component = IBRun(name=None, cfg=None, session=None)
    component._configure()
    assert('/usr/bin/ibrun' == component.launch_command)


# ------------------------------------------------------------------------------
#
@mock.patch.object(IBRun, '__init__',   return_value=None)
@mock.patch.object(IBRun, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure, mocked_raise_on):

    component = IBRun(name=None, cfg=None, session=None)

    component._log = ru.Logger('dummy')
    component._cfg = {'cores_per_node': 0}
    component._node_list = [['node1'], ['node2']]

    component.name = 'IBRun'
    component.launch_command = 'ibrun'

    test_cases = setUp('lm', 'ibrun')
    for unit, result in test_cases:
        if result == 'RuntimeError':
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(unit, None)
        elif result == 'AssertionError':
            with pytest.raises(AssertionError):
                command, hop = component.construct_command(unit, None)
        else:
            command, hop = component.construct_command(unit, None)
            assert([command, hop] == result), unit['uid']


# ------------------------------------------------------------------------------
