import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.scheduler.continuous import Continuous
import pytest
import radical.pilot.constants as rpc
import glob
import os
import shutil
import copy

try:
    import mock
except ImportError:
    from tasktest import mock

# ------------------------------------------------------------------------------
# User Input for test
resource_name = 'local.localhost'
access_schema = 'ssh'


# ------------------------------------------------------------------------------
# Setup for all tests
# Stating session id
session_id = 'rp.session.testing.local.0000'

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))


# local_sample_data = os.path.join(cur_dir, 'sample_data')
# sample_data = ['single_file.txt',
#                'single_folder',
#                'multi_folder']


# ------------------------------------------------------------------------------
# Setup to be done for every test
def setUp():
    
    # Add SAGA method to only create directories on remote - don't transfer yet!
    session = rp.Session()

    cfg = dict()
    cfg['lrms_info'] = dict()
    cfg['lrms_info']['lm_info']        = 'INFO'
    cfg['lrms_info']['cores_per_node'] = 2
    cfg['lrms_info']['gpus_per_node']  = 1
    cfg['lrms_info']['lfs_per_node']   = {'size': 5120,'path': 'abc'}
    cfg['lrms_info']['node_list']      = [['a', 1], ['b', 2], ['c', 3], 
                                          ['d', 4], ['e', 5]]
    return cfg, session


# ------------------------------------------------------------------------------
#
def nompi():

    td = dict()
    td['environment']      = dict()
    td['cpu_process_type'] = None
    td['gpu_process_type'] = None
    td['cpu_processes']    = 1
    td['cpu_threads']      = 1
    td['gpu_processes']    = 0
    td['lfs_per_process']  = 1024

    return td

# ------------------------------------------------------------------------------
# Cleanup any folders and files to leave the system state
# as prior to the test
def tearDown():
    
    rp = glob.glob('%s/rp.session.*' % os.getcwd())
    for fold in rp:
        shutil.rmtree(fold)


# ------------------------------------------------------------------------------
# Test tmgr input staging of a single file
@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_nonmpi_task_with_continuous_scheduler(mocked_init,
                                               mocked_method,
                                               mocked_profiler,
                                               mocked_raise_on):

    cfg, session = setUp()

    component = Continuous(cfg=dict(), session=session)
    component._lrms_info           = cfg['lrms_info']
    component._lrms_lm_info        = cfg['lrms_info']['lm_info']
    component._lrms_node_list      = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']
    component._lrms_lfs_per_node   = cfg['lrms_info']['lfs_per_node']
    component._tag_history = dict()

    component.nodes = []
    for node, node_uid in component._lrms_node_list:
        component.nodes.append(copy.deepcopy({
            'name' : node,
            'uid'  : node_uid,
            'cores': [rpc.FREE] * component._lrms_cores_per_node,
            'gpus' : [rpc.FREE] * component._lrms_gpus_per_node,
            'lfs'  :              component._lrms_lfs_per_node
        }))

    # Allocate first TD -- should land on first node
    td  = nompi()
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'cores_per_node': 2,
                    'gpus_per_node' : 1,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'core_map': [[0]],
                                        'name'    : 'a',
                                        'gpu_map' : [],
                                        'uid'     : 1, 
                                       'lfs'      : {'size': 1024,'path': 'abc'}
                                       }]}

    # Assert resulting node list values after first TD
    assert component.nodes == [{'uid'  : 1,
                                'name' : 'a',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 4096,'path': 'abc'}},
                               {'uid'  : 2,
                                'name' : 'b',
                                'cores': [0, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 5120,'path': 'abc'}},
                               {'uid'  : 3,
                                'name' : 'c',
                                'cores': [0, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 5120,'path': 'abc'}},
                               {'uid'  : 4,
                                'name' : 'd',
                                'cores': [0, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 5120,'path': 'abc'}},
                               {'uid'  : 5,
                                'name' : 'e',
                                'cores': [0, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 5120,'path': 'abc'}}
                               ]

    # Allocate second TD -- should land on first node
    td  = nompi()
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'cores_per_node': 2,
                    'gpus_per_node' : 1,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 1, 
                                        'name'    : 'a',
                                        'core_map': [[1]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 1024,'path': 'abc'} 
                                        }]}

    # Allocate third TD -- should land on second node
    td  = nompi()
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'gpus_per_node' : 1,
                    'cores_per_node': 2,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 2,
                                        'name'    : 'b',
                                        'core_map': [[0]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 1024,'path': 'abc'} 
                                        }]}

    # Allocate four TD -- should land on third node
    td = nompi()
    td['lfs_per_process'] = 5120
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'cores_per_node': 2,
                    'gpus_per_node' : 1,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 3,
                                        'name'    : 'c',
                                        'core_map': [[0]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 5120,'path': 'abc'} 
                                        }]}

    # Fail with ValueError if  lfs required by td is more than available
    with pytest.raises(ValueError):

        td = nompi()
        td['lfs_per_process'] = 6000
        slot = component._allocate_slot(td)

    # Max out available resources
    # Allocate two CUDs -- should land on fourth and fifth node
    td = nompi()
    td['lfs_per_process'] = 5120
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'cores_per_node': 2,
                    'gpus_per_node' : 1,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 4,
                                        'name'    : 'd',
                                        'core_map': [[0]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 5120,'path': 'abc'} 
                                        }]}

    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'cores_per_node': 2,
                    'gpus_per_node' : 1,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 5,
                                        'name'    : 'e',
                                        'core_map': [[0]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 5120,'path': 'abc'} 
                                        }]}

    # Allocate TD with to land on second node
    td = nompi()
    td['lfs_per_process'] = 4096
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'cores_per_node': 2,
                    'gpus_per_node' : 1,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 2,
                                        'name'    : 'b',
                                        'core_map': [[1]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 4096,'path': 'abc'} 
                                        }]}

    # Allocate TD with no lfs requirement
    td = nompi()
    td['lfs_per_process'] = 0
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'gpus_per_node' : 1,
                    'cores_per_node': 2,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 3,
                                        'name'    : 'c',
                                        'core_map': [[1]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 0,'path': 'abc'} 
                                        }]}

    # Deallocate slot
    component._release_slot(slot)
    assert component.nodes == [{'uid'  : 1,
                                'name' : 'a',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 3072,'path': 'abc'}},
                               {'uid'  : 2,
                                'name' : 'b',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}},
                               {'uid'  : 3,
                                'name' : 'c',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}},
                               {'uid'  : 4,
                                'name' : 'd',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}},
                               {'uid'  : 5,
                                'name' : 'e',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}}
                               ]

    # Allocate TD which cannot fit on available resources
    td = nompi()
    td['lfs_per_process'] = 5120
    slot = component._allocate_slot(td)
    assert slot == None

    # Deallocate third node
    slot = {'lm_info'       : 'INFO',
            'gpus_per_node' : 1,
            'cores_per_node': 2,
            'lfs_per_node'  : component._lrms_lfs_per_node,
            'nodes'         : [{'uid'     : 3,
                                'name'    : 'c',
                                'core_map': [[0]],
                                'gpu_map' : [],
                                'lfs'     : {'path': 'abc',
                                             'size': 5120}
                                }]}
    component._release_slot(slot)
    assert component.nodes == [{'uid'  : 1,
                                'name' : 'a',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 3072, 'path': 'abc'}
                                },
                               {'uid'  : 2,
                                'name' : 'b',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 0, 'path': 'abc'}
                                },
                               {'uid'  : 3,
                                'name' : 'c',
                                'cores': [0, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 5120, 'path': 'abc'}
                                },
                               {'uid'  : 4,
                                'name' : 'd',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0, 'path': 'abc'}
                                },
                               {'uid'  : 5,
                                'name' : 'e',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0, 'path': 'abc'}
                                }]

    # Allocate TD to run multi threaded application
    td = nompi()
    td['cpu_processes'] = 1
    td['cpu_threads']   = 2
    slot = component._allocate_slot(td)
    assert slot == {'lm_info'       : 'INFO',
                    'gpus_per_node' : 1,
                    'cores_per_node': 2,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 3,
                                        'name'    : 'c',
                                        'core_map': [[0, 1]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 1024,'path': 'abc'}
                                        }]}

    assert component.nodes == [{'uid'  : 1,
                                'name' : 'a',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 3072,'path': 'abc'}
                                },
                               {'uid'  : 2,
                                'name' : 'b',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}
                                },
                               {'uid'  : 3,
                                'name' : 'c',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 4096,'path': 'abc'}
                                },
                               {'uid'  : 4,
                                'name' : 'd',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}
                                },
                               {'uid'  : 5,
                                'name' : 'e',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}
                                }]

    # Deallocate slot
    component._release_slot(slot)

    # Allocate TD to run multi process, non-mpi application
    td = nompi()
    td['cpu_processes']   = 2
    td['cpu_threads']     = 1
    td['lfs_per_process'] = 1024
    slot = component._allocate_slot(td)

    assert slot == {'lm_info'       : 'INFO',
                    'gpus_per_node' : 1,
                    'cores_per_node': 2,
                    'lfs_per_node'  : component._lrms_lfs_per_node,
                    'nodes'         : [{'uid'     : 3,
                                        'name'    : 'c',
                                        'core_map': [[0], [1]],
                                        'gpu_map' : [],
                                        'lfs'     : {'size': 2048,'path': 'abc'}
                                        }]}

    assert component.nodes == [{'uid'  : 1,
                                'name' : 'a',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 3072,'path': 'abc'}
                                },
                               {'uid'  : 2,
                                'name' : 'b',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}
                                },
                               {'uid'  : 3,
                                'name' : 'c',
                                'cores': [1, 1],
                                'gpus' : [0],
                                'lfs'  : {'size': 3072,'path': 'abc'}
                                },
                               {'uid'  : 4,
                                'name' : 'd',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}
                                },
                               {'uid'  : 5,
                                'name' : 'e',
                                'cores': [1, 0],
                                'gpus' : [0],
                                'lfs'  : {'size': 0,'path': 'abc'}
                                }]

    tearDown()


# ------------------------------------------------------------------------------

