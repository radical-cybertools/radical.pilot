import re
import os
import glob
import shutil
import radical.pilot as rp
import radical.utils as ru
from radical.pilot.agent import lm as rpa_lm

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
def test_lm_mpirun(resource='xsede.bridges'):

    cfg, session = setUp(resource)

    component = rpa_lm.LaunchMethod.create(name=cfg['mpi_launch_method'], cfg=cfg, session=session)

    assert('mpirun' == component.launch_command)
    assert('mpi_flavor' in component)

def test_lm_jsrun(resource='ornl.summit'):

    cfg, session = setUp(resource)

    component = rpa_lm.LaunchMethod.create(name=cfg['task_launch_method'], cfg=cfg, session=session)

    # /sw/summit/xalt/1.1.3/bin/jsrun
    assert('jsrun' == os.path.basename(component.launch_command))

def test_lm_prte(resource='ornl.summit_prte'):

    cfg, session = setUp(resource)

    component = rpa_lm.LaunchMethod.create(name=cfg['task_launch_method'], cfg=cfg, session=session)

    # /sw/summit/ums/ompix/gcc/6.4.0/install/prrte-dev-withtimings/bin/prun
    assert('prun' == os.path.basename(component.launch_command))
