# pylint: disable=protected-access, unused-argument

import os
import glob
import radical.utils as ru

from .test_common import setUp

from radical.pilot.agent.launch_method.jsrun import JSRUN


try:
    import mock

except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
def tearDown():
    rs = glob.glob('%s/*.rs' % os.getcwd())
    for fold in rs:
        os.remove(fold)


# ------------------------------------------------------------------------------
#
@mock.patch.object(JSRUN, '__init__', return_value=None)
@mock.patch.object(JSRUN, '_configure',return_value='jsrun')
@mock.patch('radical.utils.raise_on')
def test_create_resource_set_file(mocked_init, mocked_configure, mocked_raise_on):

    test_cases = setUp('lm', 'jsrun')
    component  = JSRUN(name=None, cfg=None, session=None)

    for unit, _, resource_file, _ in test_cases:

        slot   = unit['slots']
        uid    = unit['uid']

        component._create_resource_set_file(slots=slot, uid=uid, sandbox='.')
        print(uid)
        with open('%s.rs' % uid) as rs_layout:
            assert rs_layout.readlines() ==  resource_file

    tearDown()


# ------------------------------------------------------------------------------
#
@mock.patch.object(JSRUN, '__init__', return_value=None)
@mock.patch.object(JSRUN, '_configure', return_value='jsrun')
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure, mocked_raise_on):

    test_cases = setUp('lm', 'jsrun')
    component  = JSRUN(name=None, cfg=None, session=None)

    component._create_resource_set_file = mock.Mock()
    component.launch_command            = 'jsrun'
    component._log                      = ru.Logger('dummy')

    for unit, result, _ , resource_filename  in test_cases:
        component._create_resource_set_file.return_value = resource_filename
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)


# ------------------------------------------------------------------------------

