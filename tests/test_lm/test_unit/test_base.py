
# pylint: disable=protected-access, unused-argument


from radical.pilot.agent.lm.base import LaunchMethod

import radical.utils as ru
import pytest

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
def test_configure():

    session = mock.Mock()
    session._log = mock.Mock()
    with pytest.raises(NotImplementedError):
        LaunchMethod(name='test', cfg={}, session=session)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(LaunchMethod,'__init__',return_value=None)
def test_get_mpi_info(mocked_init):

    lm = LaunchMethod(name=None, cfg={}, session=None)
    lm._log = mock.Mock()
    ru.sh_callout = mock.Mock()
    ru.sh_callout.side_effect = [['test',1,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    if version is None:
        assert True
    else:
        assert False
    assert flavor == 'unknown'

    ru.sh_callout.side_effect = [['test',1,1],['mpirun (Open MPI) 2.1.2\n\n\
                                  Report bugs to http://www.open-mpi.org/community/help/\n',3,0]]
    version, flavor = lm._get_mpi_info('mpirun')
    assert version == '2.1.2'
    assert flavor == 'OMPI'
