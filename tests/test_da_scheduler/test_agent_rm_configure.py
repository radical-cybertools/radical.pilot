import os
import shutil
import errno
import unittest
import json

import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.rm.slurm import Slurm

import hostlist

try:
    import mock
except ImportError:
    from unittest import mock


class TestComponentSlurmResourceManager(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        # Directory of sample config files
        self.sample_resource_directory = os.path.realpath(__file__)

        # Import ncsa resource configuration
        self.sample_resource_path = os.path.join(os.path.dirname(self.sample_resource_directory), "sample_resources.json")

        # Load test configurations
        with open(self.sample_resource_path) as fp:
            self.cfg_samples = json.load(fp)
            self.cfg_1 = self.cfg_samples['sample_resource_1']
            self.cfg_2 = self.cfg_samples['sample_resource_2']
            self.cfg_3 = self.cfg_samples['sample_resource_3']
        
        return

    @classmethod
    def tearDownClass(self):
        pass

    def setUp(self):

        # Create patches for test functions
        patcher_init = mock.patch.object(Slurm, '__init__', return_value=None)
        patcher_prof = mock.patch.object(ru.Profiler, 'prof')
        patcher_host = mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes2'])

        # Clean up patches when errors occur
        self.addCleanup(patcher_init.stop)
        self.addCleanup(patcher_prof.stop)
        self.addCleanup(patcher_host.stop)

        # Start the patches
        self.mock_init = patcher_init.start()
        self.mock_prof = patcher_prof.start()
        self.mock_prof = patcher_host.start()

        # Initialize the component before each test
        self.component                   = Slurm(cfg=None, session=None)
        self.component._log              = ru.get_logger('dummy')
        self.component.cores_per_node    = None
        self.component.gpus_per_node     = None
        self.component.lfs_per_node      = None
        self.component.lm_info           = dict()

        return

    def tearDown(self):

        # Remove the environment variables used for each test 
        del os.environ['SLURM_NODELIST']
        del os.environ['SLURM_NPROCS']
        del os.environ['SLURM_NNODES']
        del os.environ['SLURM_CPUS_ON_NODE']

        return


    def test_slurm_sample_resource_1(self):
        """
        Test Slurm with Sample Resource 1
        """

        # Set environment variables
        os.environ['SLURM_NODELIST']        = 'nodes[1-2]'
        os.environ['SLURM_NPROCS']          = '24'       
        os.environ['SLURM_NNODES']          = '2'
        os.environ['SLURM_CPUS_ON_NODE']    = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_1
        self.component._configure()
        
        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 9001)
        self.assertEqual(self.component.gpus_per_node , 99)
        self.assertEqual(self.component.lfs_per_node['path'], "not_comet/")
        self.assertEqual(self.component.lfs_per_node['size'], 1000)
        self.assertEqual(self.component.lm_info['cores_per_node'], 9001)

        return


    def test_slurm_sample_resource_2(self):
        """
        Test Slurm with Sample Resource 2
        """

        # Set environment variables
        os.environ['SLURM_NODELIST']        = 'nodes[1-2]'
        os.environ['SLURM_NPROCS']          = '24'       
        os.environ['SLURM_NNODES']          = '2'
        os.environ['SLURM_CPUS_ON_NODE']    = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_2
        self.component._configure()
        
        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node , 100)
        self.assertEqual(self.component.lfs_per_node['path'], "not_comet")
        self.assertEqual(self.component.lfs_per_node['size'], 1001)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return


    def test_slurm_sample_resource_3(self):
        """
        Test Slurm with Sample Resource 3
        """

        # Set environment variables
        os.environ['SLURM_NODELIST']        = 'nodes[1-2]'
        os.environ['SLURM_NPROCS']          = '24'       
        os.environ['SLURM_NNODES']          = '2'
        os.environ['SLURM_CPUS_ON_NODE']    = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_3
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 9003)
        self.assertEqual(self.component.gpus_per_node , 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/not_comet")
        self.assertEqual(self.component.lfs_per_node['size'], 1002)
        self.assertEqual(self.component.lm_info['cores_per_node'], 9003)

        return


if __name__ == "__main__":

    suite_slurm = unittest.TestLoader().loadTestsFromTestCase(TestComponentSlurmResourceManager)
    unittest.TextTestRunner(verbosity=2).run(suite_slurm)
