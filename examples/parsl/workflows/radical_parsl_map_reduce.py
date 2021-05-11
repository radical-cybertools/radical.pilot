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
                        resource = 'local.localhost_funcs',
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

# Map function that returns double the input integer
@python_app
def app_double(x, nproc):
    return x*2

# Reduce function that returns the sum of a list
@python_app
def app_sum(inputs, nproc):
    return sum(inputs)

# Create a list of integers
items = range(0,4)

# Map phase: apply the double *app* function to each item in list
mapped_results = []
for i in items:
    x = app_double(i, nproc =1)
    mapped_results.append(x)

# Reduce phase: apply the sum *app* function to the set of results
total = app_sum(inputs=mapped_results, nproc=1)

print(total.result())

