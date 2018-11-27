
# pylint: disable=protected-access, unused-argument

import json
import os

import radical.utils as ru

from   radical.pilot.agent.lm.fork import Fork


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
# 
def setUp():

    curdir     = os.path.dirname(os.path.abspath(__file__))
    test_cases = json.load(open('%s/test_cases_fork.json' % curdir))

    return test_cases


# ------------------------------------------------------------------------------
#
def tearDown():

    pass


# ------------------------------------------------------------------------------
#
@mock.patch.object(Fork, '__init__',   return_value=None)
@mock.patch.object(Fork, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp()

    component = Fork(name=None, cfg=None, session=None)
    component._log = ru.get_logger('dummy')
    component.launch_command = 'fork'

    for i in range(len(test_cases['trigger'])):

        cu         = test_cases['trigger'][i]
        command, _ = component.construct_command(cu, None)
        print command

        assert command == test_cases['result'][i]

    tearDown()


# ------------------------------------------------------------------------------

