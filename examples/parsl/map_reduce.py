#!/usr/bin/env python3

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
                        label        = 'RADICALExecutor',
                        resource     = 'local.localhost',
                        login_method = 'local',
                        project      = '',
                        partition    = '',
                        walltime     = 30,
                        managed      = True,
                        max_tasks    = 1024)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

# Map function that returns double the input integer
@bash_app
def app_double(x, cpu_processes=1):
    return "echo $(( 2 * {0} ))".format(x)

# Reduce function that returns the sum of a list
@python_app
def app_sum(inputs, cpu_processes=1):
    x = sum(inputs)
    return x


# Map phase: apply the double *app* function to each item in list
mapped_tasks = []
for i in range(1024):
    x = app_double(i, cpu_processes=1)
    mapped_tasks.append(x)

print(x)

# Reduce phase: apply the sum *app* function to the set of results
total = app_sum([int(x.result()) for x in mapped_tasks], cpu_processes=1)

print(total.result())

