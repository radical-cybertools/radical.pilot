import os 
import parsl
import radical.pilot as rp
from parsl import File
from parsl.config import Config
from parsl.app.app import python_app, bash_app
from radical.pilot.agent.executing.parsl_rp import RADICALExecutor as RADICALExecutor

parsl.set_stream_logger()

config = Config(
         executors=[RADICALExecutor(
                        label = 'RADICALExecutor',
                        resource = 'xsede.comet_ssh', #'local.localhost_funcs',
                        login_method = 'gsissh',
                        project = '',
                        partition = 'debug',
                        cores_per_task=1,
                        walltime = 30,
                        managed = True,
                        max_tasks = 3)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

# MPI Shell
"""
@bash_app
def mpi_shell(exe ='/bin/sh',ptype=rp.MPI, nproc=3,outputs=[],stdout= '',stderr=''):
    return '/home/aymen/mpi.sh'.format(outputs[0])

results = [] 

for i in range(3):
    out_file = "/home/aymen/mpi_shell_{0}".format(i)
    results.append(mpi_shell(outputs=[out_file]))
    mpi_shell()
"""

# MPI C
@bash_app
def mpi_C(exe ='', ptype=rp.MPI, nproc=3,outputs=[], stdout= '', stderr=''):
    return '/home/aymen/mpi_hello_world'.format(outputs[0])

results = []

for i in range(2):
    out_file = "/home/aymen/mpi_C_{0}".format(i)
    results.append(mpi_C(outputs=[out_file]))
    mpi_C()


print ("Job Status: {}".format([r.done() for r in results]))
# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))

outputs = [r.outputs[0] for r in results]
print(outputs)
