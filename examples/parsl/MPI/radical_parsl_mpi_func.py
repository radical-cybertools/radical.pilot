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
                        max_tasks = 4)
                        ],

strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def hello_mpi(pre_exec,nproc,nthrd):
    from mpi4py import MPI
    import sys

    size = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()
    name = MPI.Get_processor_name()
    sys.stdout.write(
        "Hello, World! I am process %d of %d on %s.\n"
        % (rank, size, name))


results = []
out_file = "/home/aymen/hello_mpi_{0}".format(0)
results.append(hello_mpi(pre_exec = [],nproc=3,nthrd=1))

# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))
