
import os
import json
import glob
from pprint import pprint
import radical.utils as ru
from test_common                  import setUp
from radical.pilot.agent.lm.jsrun import JSRUN


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
@mock.patch.object(JSRUN, '_configure',retunr_value='jsrun')
@mock.patch('radical.utils.raise_on')
def test_create_resource_set_file(mocked_init, mocked_method, mocked_raise_on):

    test_cases = setUp('lm', 'jsrun')
    component    = JSRUN(cfg=None, session=None)

    for unit, _, resource_file in test_cases:

        slot         = unit['slots']
        uid          = unit['uid']

        component._create_resource_set_file(slots=slot, uid=uid, sandbox='.')
        print uid
        with open('%s.rs' % uid) as rs_layout:
            assert rs_layout.readlines() ==  resource_file

    tearDown()


# ------------------------------------------------------------------------------
#
def _create_resource_set_file(self, slots, uid, sandbox):
    return 'rs_layout_cu_%06d' % (int(uid.split('_')[1]))
@mock.patch.object(JSRUN, '__init__', return_value=None)
@mock.patch.object(JSRUN, '_configure', return_value='jsrun')
@mock.patch.object(JSRUN, '_create_resource_set_file',
                           _create_resource_set_file)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure,
                           mocked_create_resource_set_file, mocked_raise_on):

    test_cases = setUp('lm', 'jsrun')
    
    component = JSRUN(cfg=None, session=None)
    component._log  = ru.get_logger('dummy')
    component.launch_command = 'jsrun'
    for unit, result,_ in test_cases:
        command, hop = component.construct_command(unit, None)
        assert([command, hop] == result)



# ------------------------------------------------------------------------------

