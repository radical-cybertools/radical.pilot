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
        self.sample_resource_dir = os.path.dirname(os.path.realpath(__file__))

        # Load sample resource test config
        self.cfg_sample = ru.read_json(os.path.join(self.sample_resource_dir, "sample_resources.json"))
        self.cfg_sample_1 = self.cfg_sample['sample_resource_1']
        self.cfg_sample_2 = self.cfg_sample['sample_resource_2']
        self.cfg_sample_3 = self.cfg_sample['sample_resource_3']

        # Directory of pilot resource config files
        self.pilot_resource_dir = '../../src/radical/pilot/configs'

        # Load xsede pilot resource config
        self.cfg_xsede = ru.read_json(os.path.join(self.pilot_resource_dir, 'resource_xsede.json'))
        self.cfg_xsede_bridges = self.cfg_xsede['bridges']
        self.cfg_xsede_comet_ssh = self.cfg_xsede['comet_ssh']
        self.cfg_xsede_comet_orte = self.cfg_xsede['comet_orte']
        self.cfg_xsede_comet_ortelib = self.cfg_xsede['comet_ortelib']
        self.cfg_xsede_comet_spark = self.cfg_xsede['comet_spark']
        self.cfg_xsede_supermic_ssh = self.cfg_xsede['supermic_ssh']
        self.cfg_xsede_supermic_orte = self.cfg_xsede['supermic_orte']
        self.cfg_xsede_supermic_ortelib = self.cfg_xsede['supermic_ortelib']
        self.cfg_xsede_supermic_spark = self.cfg_xsede['supermic_spark']

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
        self.component = Slurm(cfg=None, session=None)
        self.component._log = ru.get_logger('dummy')
        self.component.cores_per_node = None
        self.component.gpus_per_node = None
        self.component.lfs_per_node = None
        self.component.lm_info = dict()

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
        Test Slurm with sample_resource_1
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_sample_1
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 9001)
        self.assertEqual(self.component.gpus_per_node, 99)
        self.assertEqual(self.component.lfs_per_node['path'], "not_comet/")
        self.assertEqual(self.component.lfs_per_node['size'], 1000)
        self.assertEqual(self.component.lm_info['cores_per_node'], 9001)

        return

    def test_slurm_sample_resource_2(self):
        """
        Test Slurm with sample_resource_2
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_sample_2
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node, 100)
        self.assertEqual(self.component.lfs_per_node['path'], "not_comet")
        self.assertEqual(self.component.lfs_per_node['size'], 1001)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return

    def test_slurm_sample_resource_3(self):
        """
        Test Slurm with sample_resource_3
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_sample_3
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 9003)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/not_comet")
        self.assertEqual(self.component.lfs_per_node['size'], 1002)
        self.assertEqual(self.component.lm_info['cores_per_node'], 9003)

        return

    def test_slurm_xsede_bridges(self):
        """
        Test Slurm with xsede_bridges
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_bridges
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "$LOCAL")
        self.assertEqual(self.component.lfs_per_node['size'], 3713368)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return

    def test_slurm_xsede_comet_ssh(self):
        """
        Test Slurm with xsede_comet_ssh
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_comet_ssh
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/scratch/$USER/$SLURM_JOB_ID")
        self.assertEqual(self.component.lfs_per_node['size'], 176105)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return

    def test_slurm_xsede_comet_orte(self):
        """
        Test Slurm with xsede_comet_orte
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_comet_orte
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/scratch/$USER/$SLURM_JOB_ID")
        self.assertEqual(self.component.lfs_per_node['size'], 176105)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return

    def test_slurm_xsede_comet_ortelib(self):
        """
        Test Slurm with xsede_comet_ortelib
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_comet_ortelib
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/scratch/$USER/$SLURM_JOB_ID")
        self.assertEqual(self.component.lfs_per_node['size'], 176105)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return

    def test_slurm_xsede_comet_spark(self):
        """
        Test Slurm with xsede_comet_spark
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_comet_spark
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 24)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/scratch/$USER/$SLURM_JOB_ID")
        self.assertEqual(self.component.lfs_per_node['size'], 176105)
        self.assertEqual(self.component.lm_info['cores_per_node'], 24)

        return

    def test_slurm_xsede_supermic_ssh(self):
        """
        Test Slurm with Xsede supermic_ssh
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_supermic_ssh
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 20)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/var/scratch/")
        self.assertEqual(self.component.lfs_per_node['size'], 200496)
        self.assertEqual(self.component.lm_info['cores_per_node'], 20)

        return

    def test_slurm_xsede_supermic_orte(self):
        """
        Test Slurm with xsede_supermic_orte
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_supermic_orte
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 20)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/var/scratch/")
        self.assertEqual(self.component.lfs_per_node['size'], 200496)
        self.assertEqual(self.component.lm_info['cores_per_node'], 20)

        return

    def test_slurm_xsede_supermic_ortelib(self):
        """
        Test Slurm with xsede_supermic_ortelib
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_supermic_ortelib
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 20)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/var/scratch/")
        self.assertEqual(self.component.lfs_per_node['size'], 200496)
        self.assertEqual(self.component.lm_info['cores_per_node'], 20)

        return

    def test_slurm_xsede_supermic_spark(self):
        """
        Test Slurm with xsede_supermic_spark
        """

        # Set environment variables
        os.environ['SLURM_NODELIST'] = 'nodes[1-2]'
        os.environ['SLURM_NPROCS'] = '24'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        # Run component with desired configuration
        self.component._cfg = self.cfg_xsede_supermic_spark
        self.component._configure()

        # Verify configured correctly
        self.assertEqual(self.component.cores_per_node, 20)
        self.assertEqual(self.component.gpus_per_node, 0)
        self.assertEqual(self.component.lfs_per_node['path'], "/var/scratch/")
        self.assertEqual(self.component.lfs_per_node['size'], 200496)
        self.assertEqual(self.component.lm_info['cores_per_node'], 20)

        return


if __name__ == "__main__":

    test_classes = [TestComponentSlurmResourceManager]

    suites = list()
    for test_class in test_classes:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suites.append(suite)

    big_suite = unittest.TestSuite(suites)

    unittest.TextTestRunner(verbosity=2).run(big_suite)
