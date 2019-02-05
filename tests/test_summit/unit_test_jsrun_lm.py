
import os
import json
import glob

import radical.utils as ru

from radical.pilot.agent.lm.jsrun import JSRUN


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
resource_name = 'local.localhost'
access_schema = 'ssh'
session_id    = 'rp.session.testing.local.0000'

# Sample data to be staged -- available in cwd
cur_dir       = os.path.dirname(os.path.abspath(__file__))


# ------------------------------------------------------------------------------
# Setup for every test
def setUp():

    test_cases = json.load(open('unit_test_cases_jsrun_lm.json'))

    return test_cases['resource_file'],test_cases['command']


# ------------------------------------------------------------------------------
#
def tearDown():
    rs = glob.glob('%s/rs_layout_cu_*' % os.getcwd())
    for fold in rs:
        os.remove(fold)


# ------------------------------------------------------------------------------
#
@mock.patch.object(JSRUN, '__init__', return_value=None)
@mock.patch.object(JSRUN, '_configure',retunr_value='jsrun')
@mock.patch('radical.utils.raise_on')
def test_create_resource_set_file(mocked_init, mocked_method, mocked_raise_on):

    test_cases,_ = setUp()
    component    = JSRUN(cfg=None, session=None)

    for i in range(len(test_cases['trigger'])):

        slot         = test_cases['trigger'][i]['slots']
        uid          = test_cases['trigger'][i]['uid']
        file_content = test_cases['results'][i]

        component._create_resource_set_file(slots=slot, uid=uid, sandbox='.')

        with open('%s.rs' % uid) as rs_layout:
            assert rs_layout.readlines() ==  file_content

    tearDown()


# ------------------------------------------------------------------------------
#
@mock.patch.object(JSRUN, '__init__', return_value=None)
@mock.patch.object(JSRUN, '_configure', return_value='jsrun')
@mock.patch.object(JSRUN, '_create_resource_set_file',
                          return_value='rs_layout_cu_000000')
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure,
                           mocked_create_resource_set_file, mocked_raise_on):

    _, test_cases = setUp()

    component = JSRUN(cfg=None, session=None)
    component._log  = ru.get_logger('dummy')
    component.launch_command = 'jsrun'

    for i in range(len(test_cases['trigger'])):

        cu        = test_cases['trigger'][i]
        command, _ = component.construct_command(cu,None)

        assert command == test_cases['result'][i]

    tearDown()


# ------------------------------------------------------------------------------

