import os
import glob
import shutil
import radical.pilot as rp
from radical.pilot.agent import rm as rpa_rm

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
def setUp(resource):

    session      = rp.Session()
    cfg          = session.get_resource_config(resource=resource)

    return cfg, session


# ------------------------------------------------------------------------------
#
def tearDown():

    cur_dir       = os.path.dirname(os.path.abspath(__file__))
    rp = glob.glob('%s/rp.session.*' % cur_dir)

    for fold in rp:
        shutil.rmtree(fold)


# ------------------------------------------------------------------------------
#
def test_rm_create_fork():

    cfg, session = setUp('local.localhost')
    cfg['cores'] = 1
    cfg['gpus'] = 0

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    assert lrms.lrms_info == {'agent_nodes'     : {},
                              'gpus_per_node'   : 1,
                              'cores_per_node'  : 8,
                              'lfs_per_node'    : {'path': "/tmp", 'size': 1024},
                              'lm_info'         : {'version_info'    : {'FORK':
                                  {'version': '0.42', 'version_detail': 
                                  'There is no spoon'}}},
                              'mem_per_node'    : 0,
                              'name'            : 'Fork',
                              'node_list'       : [['localhost', 'localhost_0']]}
    lrms.stop()
    tearDown()


from radical.pilot.agent.rm.pbspro import PBSPro
@mock.patch.object(PBSPro, '_parse_pbspro_vnodes', return_value=['nodes1', 'nodes2'])
def test_rm_create_pbspro(mocked_parse_pbspro_vnodes):

    cfg, session = setUp('epsrc.archer_aprun')
    cfg['cores'] = 1
    cfg['gpus'] = 0

    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.pbs'
    os.environ['SAGA_PPN'] = '0'
    os.environ['NODE_COUNT'] = '2'
    os.environ['NUM_PPN'] = '4'
    os.environ['NUM_PES'] = '1'
    os.environ['PBS_JOBID'] = '482125'

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    lrms.stop()
    tearDown()


# ------------------------------------------------------------------------------

