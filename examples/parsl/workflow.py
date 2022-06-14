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
                        resource = 'local.localhost',
                        login_method = 'local',
                        project = '',
                        partition = '',
                        walltime = 30,
                        managed = True,
                        max_tasks = 1)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def wait_sleep_double(x, foo_1, foo_2, nproc):
     import time
     time.sleep(2)   # Sleep for 2 seconds
<<<<<<< HEAD
     return x*2
=======
     return x * 2
>>>>>>> d3203b568a8cd974f6094498114c056356f8bc5f

# Launch two apps, which will execute in parallel, since they do not have to
# wait on any futures
doubled_x = wait_sleep_double(10, None, None, nproc=1)
doubled_y = wait_sleep_double(10, None, None, nproc=1)

# The third app depends on the first two:
#    doubled_x   doubled_y     (2 s)
#           \     /
#           doublex_z          (2 s)
doubled_z = wait_sleep_double(10, doubled_x, doubled_y, nproc=1)

# doubled_z will be done in ~4s
print(doubled_z.result())

