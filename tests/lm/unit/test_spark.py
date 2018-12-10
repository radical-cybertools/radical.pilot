
# pylint: disable=protected-access, unused-argument

from   test_common                  import setUp
from   radical.pilot.agent.lm.spark import Spark

import radical.utils as ru

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Spark, '__init__',   return_value=None)
@mock.patch.object(Spark, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp('lm', 'spark')
    component  = Spark(cfg=None, session=None, name=None)

    component._log           = ru.get_logger('dummy')
    component.launch_command = ''

    for unit, result in test_cases:
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)


# ------------------------------------------------------------------------------

