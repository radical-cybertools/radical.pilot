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
                        resource = 'local.localhost', #'local.localhost_funcs',
                        login_method = 'local',
                        project = '',
                        partition = '',
                        walltime = 30,
                        managed = True,
                        max_tasks = 3)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@bash_app
def mpi_task(exe ='/bin/sh',ptype=rp.MPI, nproc=3,outputs=[],stdout= '',stderr=''):
    return '/home/aymen/mpi.sh'.format(outputs[0])

results = [] 

for i in range(2):
    out_file = "/home/aymen/mpi_task_{0}".format(i)
    results.append(mpi_task(outputs=[out_file]))
    mpi_task()

print ("Job Status: {}".format([r.done() for r in results]))
# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))

outputs = [r.outputs[0] for r in results]
print(outputs)
