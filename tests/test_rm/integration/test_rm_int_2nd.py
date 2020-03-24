import radical.pilot as rp
import radical.utils as ru
import subprocess

session      = rp.Session()
config = ru.read_json('examples/config.json')

def test_slurm():
    excluded_list = []
    cmd_to_run = "srun -t 00:00:10 -N 1 /bin/env|egrep 'SLURM_NODELIST|SLURM_NPROCS|SLURM_NNODES|SLURM_CPUS_ON_NODE'"
    for resource_name, values in session._rcfgs.items():
        if values['resource_manager'] != 'SLURM':
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


def test_slurm_xsede_stampede2():

    resource_group = "xsede"
    resource_name = "stampede2_srun"
    values = session._rcfgs[resource_group][resource_name]
    cfg = config["{}.{}".format(resource_group, resource_name)]
    cmd_to_run = ("{} -t 00:00:10 -N 1 -n 1 -p {} " + \
            "/bin/env").format(values['task_launch_method'].lower(),
                    values['default_queue'])

    env = subprocess.Popen(cmd_to_run.split(), stdout=subprocess.PIPE)
    actual_output = subprocess.check_output(("egrep",
        "'SLURM_NPROCS|SLURM_NNODES'"), #|SLURM_CPUS_ON_NODE'"),
        stdin=env.stdout) 
    expected_output = \
            "SLURM_NPROCS=1\nSLURM_NNODES=1\n" #SLURM_NODELIST=\nSLURM_CPUS_ON_NODE={}".format(cfg['cores'])
    assert actual_output == expected_output


def test_lsf_summit():
    pass

def test_torque():
    pass

def test_pbspro():
    pass

def test_fork():
    pass
