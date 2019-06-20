
# pylint: disable=protected-access, unused-argument

from   test_common                  import setUp
from   radical.pilot.agent.lm.spark import Spark

import radical.utils as ru

import pytest

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Spark, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
def test_configure(mocked_init, mocked_raise_on):

    component      = Spark(name=None, cfg=None, session=None)
    component._log = ru.get_logger('dummy')
    component._cfg = {'lrms_info':{'lm_info':{'launch_command':'spark-submit'}}}
    component._configure()
    assert('spark-submit' == component.launch_command)

# ------------------------------------------------------------------------------


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
    component.name           = 'Spark'

    for unit, result in test_cases:
        if result == "RuntimeError":
            with pytest.raises(RuntimeError):
                command, hop = component.construct_command(unit, None)
        else:
            command, hop = component.construct_command(unit, None)
            assert([command, hop] == result)


# ------------------------------------------------------------------------------
