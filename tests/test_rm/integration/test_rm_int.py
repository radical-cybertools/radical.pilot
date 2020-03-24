import re
import os
import glob
import shutil
import radical.pilot as rp
import radical.utils as ru
from radical.pilot.agent import ResourceManager as rpa_rm

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
def setUp(resource):

    session      = mock.Mock()
    session._log = mock.Mock()

    # e.g. xsede.comet_ssh
    r_groupname, r_name = resource.split('.')
    pilot_resource_dir = 'src/radical/pilot/configs'
    cfg_all      = ru.read_json(os.path.join(pilot_resource_dir,
        'resource_{}.json'.format(r_groupname)))
    cfg          = cfg_all[r_name]

    return cfg, session


# ------------------------------------------------------------------------------
#
def tearDown(rm_obj, session):

    rm_obj.stop()
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

    rm_obj = rpa_rm.create(name=cfg['resource_manager'], cfg=cfg, session=session)

    assert rm_obj.rm_info == {'agent_nodes'     : {},
                              'gpus_per_node'   : 1,
                              'cores_per_node'  : 8,
                              'lfs_per_node'    : {'path': "/tmp", 'size': 1024},
                              'lm_info'         : {'version_info'    : {'FORK':
                                  {'version': '0.42', 'version_detail': 
                                  'There is no spoon'}}},
                              'mem_per_node'    : 0,
                              'name'            : 'Fork',
                              'node_list'       : [['localhost', 'localhost_0']]}
    tearDown(rm_obj, session)


def test_rm_pbspro(resource='ncar.cheyenne'):

    cfg, session = setUp(resource)
    cfg['cores'] = 1
    cfg['gpus'] = 0

    assert 'PBS_NODEFILE' in os.environ
    assert 'SAGA_PPN' in os.environ
    assert 'NODE_COUNT' in os.environ
    assert 'NUM_PPN' in os.environ
    assert 'NUM_PES' in os.environ
    assert 'PBS_JOBID' in os.environ

    rm_obj = rpa_rm.create(name=cfg['resource_manager'], cfg=cfg, session=session)

    node_list = rm_obb.rm_info['node_list']
    # cheyenne at NCAR
    hostname_templates = [
            "cheyenne[0-9]{3}"]

    res = None
    for expr in hostname_templates:
        res = res or re.match(expr,node_list[0][0])
    assert res


    """
    assert rm_obj.rm_info == {'name': 'PBSPro', 
            'mem_per_node': 0, 
            'lm_info': {}, 
            'cores_per_node': 4, 
            'agent_nodes': {},
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['vnode1', 'vnode1'], ['vnode2', 'vnode2']],
            'gpus_per_node': 0}
    """

    tearDown(rm_obj, session)


def test_rm_torque(resource='xsede.supermic_ssh'):

    cfg, session = setUp(resource)
    cfg['cores'] = 1
    cfg['gpus'] = 0

    assert 'PBS_NODEFILE' in os.environ
    assert 'PBS_NCPUS' in os.environ
    assert 'PBS_NUM_PPN' in os.environ
    assert 'PBS_NUM_NODES' in os.environ

    rm_obj = rpa_rm.create(name=cfg['resource_manager'], cfg=cfg, session=session)

    node_list = rm_obj.rm_info['node_list']
    # qb373 at supermic
    hostname_templates = [
            "[a-zA-Z0-9]{5}"]

    res = None
    for expr in hostname_templates:
        res = res or re.match(expr,node_list[0][0])
    assert res

    """
    assert rm_obj.rm_info == {'name': 'Torque', 
            'mem_per_node': 0, 
            'lm_info': {}, 
            'cores_per_node': 24, 
            'agent_nodes': {},
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['nodes1', 'nodes1']],
            'gpus_per_node': 0}
    """

    tearDown(rm_obj, session)


def test_rm_lsf_summit(resource='ornl.summit'):

    cfg, session = setUp(resource)
    cfg['cores'] = 1
    cfg['gpus'] = 0

    os.environ['LSB_DJOB_HOSTFILE'] = 'tests/test_cases/rm/nodelist.lsf'

    rm_obj = rpa_rm.create(name=cfg['resource_manager'], cfg=cfg, session=session)
    assert rm_obj.rm_info == {'mem_per_node': 0, 
            'cores_per_node': 20,
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['nodes1', '1'], ['nodes2', '2']], 
            'gpus_per_socket': 3, 
            'name': 'LSF_SUMMIT', 
            'lm_info': {'cvd_id_mode':'logical'}, 
            'smt': 1, 
            'cores_per_socket': 10,
            'sockets_per_node': 2, 
            'agent_nodes': {}, 
            'gpus_per_node': 6}

    tearDown(rm_obj, session)


def test_rm_slurm(resource='xsede.wrangler_ssh'):

    cfg, session = setUp(resource)
    cfg['cores'] = 1
    cfg['gpus'] = 0

    rm_obj = rpa_rm.create(name=cfg['resource_manager'], cfg=cfg, session=session)

    assert 'SLURM_NODELIST' in os.environ
    assert 'SLURM_NPROCS' in os.environ
    assert 'SLURM_NNODES' in os.environ
    assert 'SLURM_CPUS_ON_NODE' in os.environ

    node_list = rm_obj.rm_info['node_list']
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
    assert rm_obj.rm_info == {'name': 'Slurm', 
            'mem_per_node': 0, 
            'lm_info': {'cores_per_node': 24}, 
            'cores_per_node': 24, 
            'agent_nodes': {}, 
            'lfs_per_node': {'path': None, 'size': 0}, 
            'node_list': [['nodes1', 'nodes1'], ['nodes2', 'nodes2']], 
            'gpus_per_node': 0}
    '''

    tearDown(rm_obj, session)


