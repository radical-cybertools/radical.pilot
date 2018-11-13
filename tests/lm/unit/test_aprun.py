import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.lm.aprun import APRun
import json
import os
from glob import glob
try:
    import mock
except ImportError:
    from unittest import mock

# pylint: disable=protected-access, unused-argument

# Setup to be done for every test
# -----------------------------------------------------------------------------


def setUp():

    curdir = os.path.dirname(os.path.abspath(__file__))
    test_cases = json.load(open('%s/test_cases_aprun.json' % curdir))
    session = rp.Session()

    return test_cases, session
# -----------------------------------------------------------------------------


def tearDown():

    rp_folds = glob('./rp.session.*')
    if rp_folds:
        import shutil
        for fold in rp_folds:
            shutil.rmtree(fold)

# -----------------------------------------------------------------------------


# Test Summit Scheduler construct_command method
# -----------------------------------------------------------------------------
@mock.patch.object(APRun, '__init__', return_value=None)
@mock.patch.object(APRun, '_configure', return_value=None)
@mock.patch('radical.utils.raise_on')
def test_construct_command(mocked_init, mocked_configure,
                           mocked_raise_on):

    test_cases, session = setUp()

    component = APRun(cfg=None, session=session)
    component._log = ru.get_logger('dummy')
    component.launch_command = 'aprun'
    for i in range(len(test_cases['trigger'])):
        cu = test_cases['trigger'][i]
        command, _ = component.construct_command(cu, None)
        print command
        assert command == test_cases['result'][i]

    tearDown()
