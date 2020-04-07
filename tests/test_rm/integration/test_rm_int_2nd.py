import radical.pilot as rp
import radical.utils as ru
import subprocess

session      = rp.Session()
config = ru.read_json('examples/config.json')


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


# python -m pytest tests/test_rm/integration/test_rm_int_2nd.py::test_lsf_summit
# -vvvvv
def test_lsf_summit(project="CSC393"):

    resource_group = "ornl"
    resource_name = "summit"
    values = session._rcfgs[resource_group][resource_name]
    cfg = config["{}.{}".format(resource_group, resource_name)]
    cmd_to_run = ("bsub -W 1 -nnodes 1 -P {} -q {} -Is {} " + \
            "/bin/env").format(project, values['default_queue'], values['task_launch_method'].lower())

    env = subprocess.Popen(cmd_to_run.split(), stdout=subprocess.PIPE)
    actual_output = subprocess.check_output(("egrep", "LSB_HOSTS"),
            stdin=env.stdout)
    actual_output_length = len(actual_output.split())
    expected_output_length = 42
    assert actual_output_length >= expected_output_length

