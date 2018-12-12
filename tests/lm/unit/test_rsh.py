
# pylint: disable=protected-access, unused-argument

import os

from   test_common                 import setUp
from   radical.pilot.agent.lm.rsh import RSH
import pytest

import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(RSH, '__init__',   return_value=None)
@mock.patch.object(RSH, '_configure', return_value=None)
@mock.patch.dict(os.environ,{'PATH':'test_path'})
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'rsh')
    component  = RSH(name=None, cfg=None, session=None)

    component._log           = ru.get_logger('dummy')
    component.name           = 'RSH'
    component.mpi_flavor     = None
    component.launch_command = 'rsh'

    for unit, result in test_cases:
        if result == "ValueError":
            with pytest.raises(ValueError):
                command, hop = component.construct_command(unit, None)
        elif result == "RuntimeError":
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(unit, 1)
        else:
            command, hop = component.construct_command(unit, 1)
            assert([command, hop] == result)


# ------------------------------------------------------------------------------

