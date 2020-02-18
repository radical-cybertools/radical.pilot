
# pylint: disable=protected-access, unused-argument

from   .test_common                 import setUp
from   radical.pilot.agent.launch_method.fork import Fork

import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Fork, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
def test_configure(mocked_init, mocked_raise_on):

    component = Fork(name=None, cfg=None, session=None)
    component._configure()
    assert('' == component.launch_command)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(Fork, '__init__',   return_value=None)
@mock.patch.object(Fork, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init,
                           mocked_configure,
                           mocked_raise_on):

    test_cases     = setUp('lm', 'fork')
    component      = Fork(name=None, cfg=None, session=None)
    component._log = ru.Logger('dummy')

    for unit, result in test_cases:
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)


# ------------------------------------------------------------------------------

