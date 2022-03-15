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
                        max_tasks = 2)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

# Map function that returns double the input integer
@bash_app
def app_double(x, nproc=1):
    return "echo $(( 2 * {0} ))".format(x)

# Reduce function that returns the sum of a list
@python_app
def app_sum(inputs, nproc=1):
    x = sum(inputs)
    print(x)

# Create a list of integers
items = range(0,4)

# Map phase: apply the double *app* function to each item in list
mapped_results = []
for i in items:
    x = app_double(i, nproc =1)
    for res in x.result():
        out = res.strip("\n")
        mapped_results.append(eval(out))

# Reduce phase: apply the sum *app* function to the set of results
total = app_sum(mapped_results, nproc=1)

print(total.result())

