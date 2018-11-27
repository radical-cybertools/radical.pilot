
# pylint: disable=protected-access, unused-argument

import json
import os

import radical.utils as ru

from   radical.pilot.agent.lm.orte import ORTE


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
# 
def setUp():

    curdir     = os.path.dirname(os.path.abspath(__file__))
    test_cases = ru.read_json('%s/test_cases_orte.json' % curdir)

    return test_cases


# ------------------------------------------------------------------------------
#
def tearDown():

    pass


# ------------------------------------------------------------------------------
#
@mock.patch.object(ORTE, '__init__',   return_value=None)
@mock.patch.object(ORTE, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, 
                           mocked_configure,
                           mocked_raise_on):

    test_cases = setUp()

    component = ORTE(name=None, cfg=None, session=None)
    component.name = 'orte'
    component._log = ru.get_logger('dummy')
    component.launch_command = 'orte'

    i = 0
    for i in range(len(test_cases['trigger'])):

        cu         = test_cases['trigger'][i]
        cu['uid']  = 'unit.%06d' % i
        command, _ = component.construct_command(cu, None)
        print command
        print test_cases['result'][i]

        assert command == test_cases['result'][i]

        i += 1

    tearDown()


# ------------------------------------------------------------------------------

