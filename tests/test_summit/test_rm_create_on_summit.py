import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent import rm as rpa_rm
import os
import glob
import shutil

try:
    import mock
except ImportError:
    from unittest import mock

# User Input for test
# -----------------------------------------------------------------------------------------------------------------------
resource_name = 'ornl.summit'
access_schema = 'fork'
# -----------------------------------------------------------------------------------------------------------------------

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))
# -----------------------------------------------------------------------------------------------------------------------

# Setup to be done for every test
# -----------------------------------------------------------------------------------------------------------------------


def setUp():

    session = rp.Session()
    cfg = session.get_resource_config(resource='ornl.summitdev')
    cfg["cores"] = 40

    return cfg, session
# -----------------------------------------------------------------------------------------------------------------------


def tearDown():
    rp = glob.glob('%s/rp.session.*' % cur_dir)
    for fold in rp:
        shutil.rmtree(fold)
# -----------------------------------------------------------------------------------------------------------------------


def test_rm_create_on_summit():

    cfg, session = setUp()

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    # Check if the lrms object has the expected lrms_info dict
    # This dict is required by the scheduler and lm
    assert lrms.lrms_info['agent_nodes'] == {}
    assert lrms.lrms_info['cores_per_socket'] == 10
    assert lrms.lrms_info['gpus_per_socket'] == 3
    assert lrms.lrms_info['lfs_per_node'] == {'path': None, 'size': 0}
    assert lrms.lrms_info['lm_info'] == {}
    assert lrms.lrms_info['name'] == 'LSF_SUMMIT'
    assert lrms.lrms_info['sockets_per_node'] == 2

    for x, y in lrms.lrms_info['node_list']:
        assert 'summitdev-' in x
        assert isinstance(y,int)
    
    # tearDown() -- failing on Summitdev
