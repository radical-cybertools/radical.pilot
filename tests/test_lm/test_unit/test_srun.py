# pylint: disable=protected-access, unused-argument

import pytest
try:
    import mock
except ImportError:
    from unittest import mock

import radical.utils as ru
from radical.pilot.agent.launch_method.srun import Srun

from .test_common import setUp


# ------------------------------------------------------------------------------
#
@mock.patch.object(Srun, '__init__', return_value=None)
@mock.patch('radical.utils.which', return_value='/bin/srun')
@mock.patch('radical.utils.sh_callout', return_value=['19.05.2', '', 0])
def test_configure(mocked_init, mocked_which, mocked_sh_callout):

    component = Srun(name=None, cfg=None, session=None)
    component._log = ru.Logger('dummy')
    component._configure()
    assert('/bin/srun' == component.launch_command)
    assert('19.05.2' == component._version)


# ------------------------------------------------------------------------------
#
@mock.patch.object(Srun, '__init__',   return_value=None)
@mock.patch('radical.utils.which', return_value=None)
@mock.patch('radical.utils.sh_callout', return_value=['', '', 1])
@mock.patch('radical.utils.raise_on')
def test_configure_fail(mocked_init, mocked_which, mocked_sh_callout,
                        mocked_raise_on):

    component = Srun(name=None, cfg=None, session=None)
    with pytest.raises(RuntimeError):
        component._configure()


# ------------------------------------------------------------------------------
#
@mock.patch.object(Srun, '__init__', return_value=None)
@mock.patch.object(Srun, '_configure', return_value=None)
def test_construct_command(mocked_init, mocked_configure):

    component = Srun(name=None, cfg=None, session=None)

    component._log = ru.Logger('dummy')
    component._cfg = {}

    component.name = 'srun'
    component.launch_command = '/bin/srun'

    test_cases = setUp('lm', 'srun')
    for unit, result in test_cases:

        if result != "RuntimeError":
            command, hop = component.construct_command(unit, None)
            assert([command, hop] == result), unit['uid']


# ------------------------------------------------------------------------------
