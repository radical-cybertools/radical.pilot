import os
import parsl
import radical.pilot as rp
from parsl.app.app import python_app, bash_app
from parsl import File

from parsl.config import Config
from parsl.channels import SSHChannel
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.addresses import address_by_query
from parsl.addresses import address_by_hostname
from parsl.executors import HighThroughputExecutor
from parsl.executors.high_throughput import interchange
from radical.pilot.agent.executing.parsl_rp import RADICALExecutor as RADICALExecutor

"""
This example illustrates how we assign different executors 
for different fucntions:

    generate  -----> RADICALEXECUTOR 
    concat    -----> HighThroughputExecutor
    total     -----> HighThroughputExecutor

** Still under testing!
"""


config = Config(
    executors=[
        HighThroughputExecutor(
            label='Comet_HTEX_multinode',
            worker_debug=True,
            address='js-17-185.jetstream-cloud.org',
            max_workers=24,
            cores_per_worker=1,
            worker_logdir_root = '/home/aymen/parsl_scripts',
            interchange_address='comet-ln2.sdsc.edu',
            interchange_port_range=(50100, 50400),
            worker_port_range=(50500, 51000),
            provider=SlurmProvider(
                launcher=SrunLauncher(),
                channel=SSHChannel(
                    hostname='comet-ln2.sdsc.edu',
                    username='',     
                    password='',
                    script_dir='/home/aymen/parsl_scripts',),
                    
                scheduler_options='',
                worker_init='source /home/aymen/ve/parsl-env/bin/activate',
                partition = "compute",
                walltime="01:00:00",
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=1,
                parallelism=24,
            ),
            working_dir="/home/aymen/parsl_scripts",
        ),
        RADICALExecutor(
                        label = 'RADICALExecutor',
                        resource = 'xsede.comet_ssh',
                        login_method = 'gsissh',
                        project = '',
                        partition = 'debug',
                        walltime = 30,
                        managed = True,
                        max_tasks = 3)
    ],
strategy='simple',
usage_tracking=True)
parsl.load(config)

@bash_app(executors=["RADICALExecutor"])
def generate(nproc=1, ptype= None, outputs=[]):
    return 'echo $(( RANDOM )) &> {}'.format(outputs[0].filepath)


@bash_app(executors=["Comet_HTEX_multinode"])
def concat(inputs=[], outputs=[]):
    return "cat {0} > {1}".format(" ".join(i.filepath for i in inputs), outputs[0].filepath)


@python_app(executors=["Comet_HTEX_multinode"])
def total(inputs=[]):
    total = 0
    with open(inputs[0], 'r') as f:
        for l in f:
            total += int(l)
    return total

output_files = []
for i in range (5):
    output_files.append(generate(outputs=[File('random-%s.txt' % i)]))

# Concatenate the files into a single file 
# We are failing here with depndecy error between generate <-> concat
cc = concat(inputs=[i.outputs[0] for i in output_files], outputs=[File("all.txt")]) 

# Calculate the sum of the random numbers
result = total(inputs=[cc.outputs[0]])

print (result.result())