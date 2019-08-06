import re
import os
import glob
import shutil
import radical.pilot as rp
from radical.pilot.agent import rm as rpa_rm
from radical.pilot.agent.rm.pbspro import PBSPro

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
def tearDown(lrms, session):

    lrms.stop()
    session.close()

    cur_dir = os.getcwd()
    rp = glob.glob('%s/rp.session.*' % cur_dir)

    #for fold in rp:
    #    shutil.rmtree(fold)

    try:
        os.unlink('radical.saga.api.log')
        os.unlink('radical.saga.log')
        os.unlink('radical.utils.log')
    except:
        pass


# ------------------------------------------------------------------------------
#
def test_rm_fork():

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
    tearDown(lrms, session)


#@mock.patch.object(PBSPro, '_parse_pbspro_vnodes', return_value=['nodes1', 'nodes2'])
@mock.patch('subprocess.check_output', return_value='exec_vnode = (vnode1:cpu=3)+(vnode2:cpu=2)')
def test_rm_pbspro(mocked_parse_pbspro_vnodes):

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

    assert lrms.lrms_info == {'name': 'PBSPro', 
            'mem_per_node': 0, 
            'lm_info': {}, 
            'cores_per_node': 4, 
            'agent_nodes': {},
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['vnode1', 'vnode1'], ['vnode2', 'vnode2']],
            'gpus_per_node': 0}

    tearDown(lrms, session)


@mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes1'])
def test_rm_torque(mocked_expand_hoslist):

    cfg, session = setUp('nersc.hopper_aprun')
    cfg['cores'] = 1
    cfg['gpus'] = 0

    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.torque'
    os.environ['PBS_NCPUS'] = '2'
    os.environ['PBS_NUM_PPN'] = '4'
    os.environ['PBS_NUM_NODES'] = '2'

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    assert lrms.lrms_info == {'name': 'Torque', 
            'mem_per_node': 0, 
            'lm_info': {}, 
            'cores_per_node': 24, 
            'agent_nodes': {},
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['nodes1', 'nodes1']],
            'gpus_per_node': 0}

    tearDown(lrms, session)


def test_rm_lsf_summit():

    cfg, session = setUp('ornl.summit')
    cfg['cores'] = 1
    cfg['gpus'] = 0

    os.environ['LSB_DJOB_HOSTFILE'] = 'tests/test_cases/rm/nodelist.lsf'

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)
    assert lrms.lrms_info == {'mem_per_node': 0, 
            'cores_per_node': 20,
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['nodes1', '1'], ['nodes2', '2']], 
            'gpus_per_socket': 3, 
            'name': 'LSF_SUMMIT', 
            'lm_info': {}, 
            'smt': 1, 
            'cores_per_socket': 10,
            'sockets_per_node': 2, 
            'agent_nodes': {}, 
            'gpus_per_node': 6}

    tearDown(lrms, session)


def test_rm_slurm(resource='xsede.wrangler_ssh'):

    cfg, session = setUp(resource)
    cfg['cores'] = 1
    cfg['gpus'] = 0

    lrms = rpa_rm.RM.create(name=cfg['lrms'], cfg=cfg, session=session)

    assert 'SLURM_NODELIST' in os.environ
    assert 'SLURM_NPROCS' in os.environ
    assert 'SLURM_NNODES' in os.environ
    assert 'SLURM_CPUS_ON_NODE' in os.environ

    node_list = lrms.lrms_info['node_list']
    # comet-03-03 at sdsc
    # r342 at psc
    # c456-041 at tacc (wrangler, stampede2, frontera)
    # tiger-h26c2n22 at princeton
    hostname_templates = [
            "[a-zA-Z]+-[0-9]{2}-[0-9]{2}",
            "[a-zA-Z0-9]{4}",
            "[a-zA-Z]{1}[0-9]{3}-[0-9]{3}",
            "[a-zA-Z]{5}-[a-zA-Z0-9]{8}"]

    res = None
    for expr in hostname_templates:
        res = res or re.match(expr,node_list[0][0])
    assert res
    '''
    assert lrms.lrms_info == {'name': 'Slurm', 
            'mem_per_node': 0, 
            'lm_info': {'cores_per_node': 24}, 
            'cores_per_node': 24, 
            'agent_nodes': {}, 
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['nodes1', 'nodes1'], ['nodes2', 'nodes2']], 
            'gpus_per_node': 0}
    '''

    tearDown(lrms, session)


