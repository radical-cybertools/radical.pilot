
# pylint: disable=protected-access, unused-argument

from test_common                  import setUp
from radical.pilot.agent.lm.aprun import APRun

import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(APRun, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('radical.utils.which', return_value='/usr/bin/aprun')
def test_configure(mocked_init, mocked_raise_on, mocked_which):

    component = APRun(name=None, cfg=None, session=None)
    component._configure()
    assert('/usr/bin/aprun' == component.launch_command)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(APRun, '__init__',   return_value=None)
@mock.patch.object(APRun, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'aprun')
    component  = APRun(name=None, cfg=None, session=None)

    component.launch_command = 'aprun'
    component.name           = 'aprun'
    component._log           = ru.Logger('dummy')

    for unit, result in test_cases:
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)

# ------------------------------------------------------------------------------

