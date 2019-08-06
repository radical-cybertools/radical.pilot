import radical.pilot as rp
import radical.utils as ru
import subprocess

session      = rp.Session()
config = ru.read_json('../../../examples/config.json')

def test_slurm():
    excluded_list = []
    cmd_to_run = "srun -t 00:00:10 -N 1 /bin/env|egrep 'SLURM_NODELIST|SLURM_NPROCS|SLURM_NNODES|SLURM_CPUS_ON_NODE'"
    for resource_name, values in session._resource_configs.items():
        if values['lrms'] != 'SLURM':
            """
            xsede.frontera
            xsede.comet_ssh
            xsede.bridges
            fub.allegro_rsh
            xsede.stampede2_ssh
            xsede.wrangler_ssh
            xsede.wrangler_spark
            xsede.comet_orte
            xsede.comet_ortelib
            das5.fs1_ssh
            vtarc_dt.stampede_ssh
            princeton.tiger_cpu
            xsede.comet_spark
            princeton.tiger_gpu
            """
            continue
        if resource_name in excluded_list:
            continue

        schema = config[resource_name]
        job_manager_endpoint = values[schema]['job_manager_endpoint'] # slurm+ssh://
        m=re.search('(\w+)\+(\w+)://([a-zA-Z.-_?:%]+)*$', job_manager_endpoint)
        rm, schema, hostname = m.group(1), m.group(2), m.group(3)
        cmd = "{} {} \"{}\"".format(schema, hostname, cmd_to_run)
        actual_output = subprocess.check_output(cmd.split())
        expected_output = "SLURM_NPROCS=1\nSLURM_NNODES=1\nSLURM_NODELIST=\nSLURM_CPUS_ON_NODE={}".format(values['cores_per_node'])
        assert actual_output == expected_output

def test_lsf_summit():
    pass

def test_torque():
    pass

def test_pbspro():
    pass

def test_fork():
    pass
