
# pylint: disable=protected-access, unused-argument


from test_common                 import setUp
from radical.pilot.agent.lm.base import LaunchMethod

import radical.utils as ru
import radical.pilot as rp
import pytest

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(LaunchMethod, '_configure', return_value=None)
def test_init(mocked_configure):
    session = rp.Session()
    lm = LaunchMethod(name='test', cfg={}, session=session)
    assert lm.name      == 'test'
    assert lm._cfg         == {}
    assert lm._session     == session
    assert lm._log         == session._log
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
#
@mock.patch.object(LaunchMethod, '_configure', return_value=None)
def test_create(mocked_configure):
    session = rp.Session()

    from radical.pilot.agent.lm.aprun          import APRun
    from radical.pilot.agent.lm.ccmrun         import CCMRun
    from radical.pilot.agent.lm.fork           import Fork
    from radical.pilot.agent.lm.ibrun          import IBRun
    from radical.pilot.agent.lm.mpiexec        import MPIExec
    from radical.pilot.agent.lm.mpirun         import MPIRun
    from radical.pilot.agent.lm.orte           import ORTE
    from radical.pilot.agent.lm.orte_lib       import ORTELib
    from radical.pilot.agent.lm.rsh            import RSH
    from radical.pilot.agent.lm.ssh            import SSH
    from radical.pilot.agent.lm.yarn           import Yarn
    from radical.pilot.agent.lm.spark          import Spark

    LM_NAME_APRUN         = 'APRUN'
    LM_NAME_CCMRUN        = 'CCMRUN'
    LM_NAME_FORK          = 'FORK'
    LM_NAME_IBRUN         = 'IBRUN'
    LM_NAME_MPIEXEC       = 'MPIEXEC'
    LM_NAME_MPIRUN        = 'MPIRUN'
    LM_NAME_MPIRUN_MPT    = 'MPIRUN_MPT'
    LM_NAME_MPIRUN_CCMRUN = 'MPIRUN_CCMRUN'
    LM_NAME_MPIRUN_DPLACE = 'MPIRUN_DPLACE'
    LM_NAME_MPIRUN_RSH    = 'MPIRUN_RSH'
    LM_NAME_ORTE          = 'ORTE'
    LM_NAME_ORTE_LIB      = 'ORTE_LIB'
    LM_NAME_RSH           = 'RSH'
    LM_NAME_SSH           = 'SSH'
    LM_NAME_YARN          = 'YARN'
    LM_NAME_SPARK         = 'SPARK'

    lm_types = {
                LM_NAME_APRUN         : APRun,
                LM_NAME_CCMRUN        : CCMRun,
                LM_NAME_FORK          : Fork,
                LM_NAME_IBRUN         : IBRun,
                LM_NAME_MPIEXEC       : MPIExec,
                LM_NAME_MPIRUN        : MPIRun,
                LM_NAME_MPIRUN_CCMRUN : MPIRun,
                LM_NAME_MPIRUN_RSH    : MPIRun,
                LM_NAME_MPIRUN_MPT    : MPIRun,
                LM_NAME_MPIRUN_DPLACE : MPIRun,
                LM_NAME_ORTE          : ORTE,
                LM_NAME_ORTE_LIB      : ORTELib,
                LM_NAME_RSH           : RSH,
                LM_NAME_SSH           : SSH,
                LM_NAME_YARN          : Yarn,
                LM_NAME_SPARK         : Spark
            }

    for key,val in lm_types.iteritems():

        print key, val
        lm = LaunchMethod.create(name=key, cfg={}, session=session)
        print lm
        assert isinstance(lm, val)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_configure():

    session = rp.Session()
    with pytest.raises(NotImplementedError):
        lm = LaunchMethod(name='test', cfg={}, session=session)
# ------------------------------------------------------------------------------