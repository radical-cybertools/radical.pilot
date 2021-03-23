import os 
import parsl
import radical.pilot as rp
from parsl import File
from parsl.config import Config
from parsl.app.app import python_app, bash_app
from radical.pilot.agent.executing.parsl_rp import RADICALExecutor as RADICALExecutor

parsl.set_stream_logger()

MAX_TASKS = 1728

config = Config(
         executors=[RADICALExecutor(
                        label = 'RADICALExecutor',
                        resource = 'xsede.comet_ssh_funcs',
                        login_method = 'gsissh',
                        project = 'unc100',
                        partition = 'compute', 
                        walltime = 60,
                        managed = True,
                        max_tasks = MAX_TASKS,
                        gpus = 0)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def rate_test(nproc):
    import time
    time.sleep(5)
    return True

results =  []

# submit tasks equal to the number of avilable core
for i in range(MAX_TASKS):
    x = rate_test(nproc=1)
    results.append(x)

[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))
