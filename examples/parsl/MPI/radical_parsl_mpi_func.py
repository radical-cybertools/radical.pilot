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
                        resource = 'local.localhost_funcs_mpi',
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
def hello_mpi(x, ptype=str,
                 nproc=int,
                 nthrd=int):
    from mpi4py import MPI

    ec = os.system('/bin/echo "on %s print %s"' % (MPI.COMM_WORLD.rank, x))
    ret = 'on %s print %s' % (MPI.COMM_WORLD.rank, x)
    sys.stdout.write('sys %s\n' % ret)
    sys.stdout.flush()

    return ec


results = []
jobs    = [47,18,99,10,72,25,7,29,74,2,81,96,26,67,15,17,51,64,38,29,48]
out_file = "/home/aymen/hello_mpi_{0}".format(0)
results.append(hello_mpi(jobs, ptype=rp.MPI_FUNC, nproc=2, nthrd=1))

# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))
