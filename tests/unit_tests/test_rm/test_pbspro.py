# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import warnings

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.pbspro import PBSPro


class TestPBSPro(TestCase):
    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch.object(PBSPro, '_parse_pbspro_vnodes',
                               return_value=['nodes1', 'nodes2'])
    def test_configure(self, mocked_init, mocked_raise_on,
                       mocked_parse_pbspro_vnodes):

        # Test 1 no config file
        base = os.path.dirname(__file__) + '/../'
        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.pbs' % base
        os.environ['SAGA_PPN']     = '0'
        os.environ['NODE_COUNT']   = '2'
        os.environ['NUM_PPN']      = '4'
        os.environ['NUM_PES']      = '1'
        os.environ['PBS_JOBID']    = '482125'

        component = PBSPro(cfg=None, session=None)
        component.name = 'PBSPro'
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component._configure()

        self.assertEqual(component.node_list, [['nodes1','nodes1'],['nodes2','nodes2']])
        self.assertEqual(component.cores_per_node, 4)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})

        # Test 2 no config file
        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.pbs' % base
        os.environ['SAGA_PPN']     = '0'
        os.environ['NODE_COUNT']   = '2'
        os.environ['NUM_PPN']      = '4'
        os.environ['NUM_PES']      = '1'
        os.environ['PBS_JOBID']    = '482125'

        component = PBSPro(cfg=None, session=None)
        component.name = 'PBSPro'
        component._log = ru.Logger('dummy')
        component._cfg = {'cores_per_node'   : 4,
                          'gpus_per_node'    : 1,
                          'lfs_path_per_node': 'test/',
                          'lfs_size_per_node': 100}
        component.lm_info = {'cores_per_node': None}
        component._configure()

        self.assertEqual(component.node_list, [['nodes1', 'nodes1'],['nodes2','nodes2']])
        self.assertEqual(component.cores_per_node, 4)
        self.assertEqual(component.gpus_per_node, 1)
        self.assertEqual(component.lfs_per_node, {'path': 'test/', 'size': 100})


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch.object(PBSPro, '_parse_pbspro_vnodes',
                               return_value=['nodes1', 'nodes2'])
    def test_configure_error(self, mocked_init, mocked_raise_on,
                             mocked_parse_pbspro_vnodes):

        # Test 1 no config file check nodefile
        base = os.path.dirname(__file__) + '/../'
        if 'PBS_NODEFILE' in os.environ:
            del os.environ['PBS_NODEFILE']

        os.environ['SAGA_PPN']   = '0'
        os.environ['NODE_COUNT'] = '2'
        os.environ['NUM_PPN']    = '4'
        os.environ['NUM_PES']    = '1'
        os.environ['PBS_JOBID']  = '482125'

        component = PBSPro(cfg=None, session=None)
        component.name = 'PBSPro'
        component._log = ru.Logger('dummy')
        component._cfg = {}

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 2 check Number of Processors per Node
        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.pbs' % base

        if 'NUM_PPN'  in os.environ: del os.environ['NUM_PPN']
        if 'SAGA_PPN' in os.environ: del os.environ['SAGA_PPN']

        os.environ['NODE_COUNT'] = '2'
        os.environ['NUM_PES']    = '1'
        os.environ['PBS_JOBID']  = '482125'

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 3 check Number of Nodes allocated
        if 'NODE_COUNT' in os.environ: del os.environ['NODE_COUNT']

        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.pbs' % base
        os.environ['SAGA_PPN']     = '0'
        os.environ['NUM_PPN']      = '4'
        os.environ['NUM_PES']      = '1'
        os.environ['PBS_JOBID']    = '482125'

        with self.assertWarns(RuntimeWarning):
            component._configure()
            warnings.warn("NODE_COUNT not set!", RuntimeWarning)

        # Test 4 check Number of Parallel Environments
        if 'NUM_PES' in os.environ: del os.environ['NUM_PES']

        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.pbs' % base
        os.environ['NUM_PPN']      = '4'
        os.environ['SAGA_PPN']     = '0'
        os.environ['NODE_COUNT']   = '2'
        os.environ['PBS_JOBID']    = '482125'

        with self.assertWarns(RuntimeWarning):
            component._configure()
            warnings.warn("NUM_PES not set!", RuntimeWarning)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch.object(PBSPro, '_configure', return_value=None)
    @mock.patch('subprocess.check_output',
                 return_value='exec_vnode = (vnode1:cpu=3)+(vnode2:cpu=2)')
    def test_parse_pbspro_vnodes(self, mocked_init, mocked_raise_on,
                                 mocked_configure, mocked_subproc):
        # Test 1 no config file JOB_ID
        os.environ['PBS_JOBID'] = '482125'
        component = PBSPro(cfg=None, session=None)
        component.name = 'PBSPro'
        component._log = ru.Logger('dummy')
        component._parse_pbspro_vnodes()


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch.object(PBSPro, '_configure', return_value=None)
    @mock.patch('subprocess.check_output',
                return_value='exec_vnode = (vnode1:cpu=3)+(vnode2:cpu=2)')
    def test_parse_pbspro_vnodes_error(self, mocked_init, mocked_raise_on,
                                       mocked_configure, mocked_subproc):
        # Test 1 check JOB_ID
        if 'PBS_JOBID' in os.environ:
            del os.environ['PBS_JOBID']

        component = PBSPro(cfg=None, session=None)
        component._cfg = {}
        component.name = 'PBSPro'
        component._log = ru.Logger('dummy')

        with self.assertRaises(RuntimeError):
            component._parse_pbspro_vnodes()


if __name__ == '__main__':

    tc = TestPBSPro()
    tc.test_configure()
    tc.test_configure_error()
    tc.test_parse_pbspro_vnodes_error()
    tc.test_parse_pbspro_vnodes()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
