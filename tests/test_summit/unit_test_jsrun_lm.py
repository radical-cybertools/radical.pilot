import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.jsrun import JSRUN
import pytest
import glob
import os
import json

try:
    import mock
except ImportError:
    from unittest import mock

# User Input for test
#-----------------------------------------------------------------------------------------------------------------------
resource_name = 'local.localhost'
access_schema = 'ssh'
#-----------------------------------------------------------------------------------------------------------------------


# Setup for all tests
#-----------------------------------------------------------------------------------------------------------------------
# Stating session id
session_id = 'rp.session.testing.local.0000'

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))
# local_sample_data = os.path.join(cur_dir, 'sample_data')
# sample_data = [
#     'single_file.txt',
#     'single_folder',
#     'multi_folder'
# ]
#-----------------------------------------------------------------------------------------------------------------------


# Setup to be done for every test
#-----------------------------------------------------------------------------------------------------------------------
def setUp():

    test_cases = json.load(open('test_cases_jsrun_lm.json'))

    return test_cases
#-----------------------------------------------------------------------------------------------------------------------

def tearDown():
    rs = glob.glob('%s/rs_layout_cu_*' % os.getcwd())
    for fold in rs:
        os.remove(fold)
#-----------------------------------------------------------------------------------------------------------------------

# Test Summit Scheduler allocate_slot method
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(JSRUN, '__init__', return_value=None)
@mock.patch.object(JSRUN, '_configure',retunr_value='jsrun')
@mock.patch('radical.utils.raise_on')
def test_create_resource_set_file(
        mocked_init,
        mocked_method,
        mocked_raise_on):

    test_cases = setUp()

    component = JSRUN(cfg=None, session=None)

    # pprint(component.nodes)

    for i in range(len(test_cases['trigger'])):
        slot = test_cases['trigger'][i]['slots']
        uid =  test_cases['trigger'][i]['uid']
        file_content = test_cases['results'][i]
        component._create_resource_set_file(slots = slot, cuid = uid)
        with open('rs_layout_cu_%06d' % uid) as rs_layout:
            assert rs_layout.readlines() ==  file_content

    tearDown()
#-----------------------------------------------------------------------------------------------------------------------
