# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import pytest
import radical.utils as ru
import radical.pilot
from radical.pilot.agent.scheduler.hombre import Hombre

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Hombre, '__init__', return_value=None)
def test_configure(mocked_init):
    component = Hombre()
    component._cfg = {'oversubscribe': True}
    component._configure()
    
    assert component._oversubscribe
    assert not component._configured
    assert isinstance(component.free, list)

    component = Hombre()
    component._cfg = {'oversubscribe': False}

    with pytest.raises(ValueError):
        component._configure()


# ------------------------------------------------------------------------------
#
@mock.patch.object(Hombre, '__init__', return_value=None)
@mock.patch.object(Hombre, '_delayed_configure', return_value=None)
@mock.patch.object(Hombre, '_find_slots', return_value={'nodes' : [1],
                                                        'cores_per_node': 1,
                                                        'gpus_per_node' : 1,
                                                        'lm_info'       : None,
                                                        'ncblocks'      : 0,
                                                        'ngblocks'      : 0})
def test_allocate_slot(mocked_init, mocked_delayed_configure,
                       mocked_find_slots):
    component = Hombre()
    component._log = mock.Mock()
    component._log.debug = mock.Mock(return_value=True)
    component.chunk = {'cpu_processes'    : 1,
                       'cpu_process_type' : 'POSIX',
                       'cpu_threads'      : 1,
                       'cpu_thread_type'  : 'OpenMP',

                       'gpu_processes'    : 1,
                       'gpu_process_type' : 'POSIX',
                       'gpu_threads'      : 1,
                       'gpu_thread_type'  : 'OpenMP',
                       }
    slot = component._allocate_slot({'cpu_processes'    : 1,
                                     'cpu_process_type' : 'POSIX',
                                     'cpu_threads'      : 1,
                                     'cpu_thread_type'  : 'OpenMP',
                                     'gpu_processes'    : 1,
                                     'gpu_process_type' : 'POSIX',
                                     'gpu_threads'      : 1,
                                     'gpu_thread_type'  : 'OpenMP',
                                     })
    assert slot == {'nodes' : [1],
                    'cores_per_node': 1,
                    'gpus_per_node' : 1,
                    'lm_info'       : None,
                    'ncblocks'      : 0,
                    'ngblocks'      : 0}

    with pytest.raises(ValueError):
        component._allocate_slot({'cpu_processes'    : 1,
                                  'cpu_process_type' : 'POSIX',
                                  'cpu_threads'      : 1,
                                  'cpu_thread_type'  : 'OpenMP',
                                  'gpu_processes'    : 2,
                                  'gpu_process_type' : 'POSIX',
                                  'gpu_threads'      : 1,
                                  'gpu_thread_type'  : 'OpenMP',
                                  })

    with pytest.raises(ValueError):
        component._allocate_slot({'cpu_processes'    : 1,
                                  'cpu_process_type' : 'MPI',
                                  'cpu_threads'      : 1,
                                  'cpu_thread_type'  : 'OpenMP',
                                  'gpu_processes'    : 1,
                                  'gpu_process_type' : 'POSIX',
                                  'gpu_threads'      : 1,
                                  'gpu_thread_type'  : 'OpenMP',
                                  })
