#!/usr/bin/env python3

import parsl

from parsl.config import Config
from parsl.app.app import python_app
from radical.pilot.agent.executing.parsl_rp import RADICALExecutor as RADICALExecutor

parsl.set_stream_logger()

config = Config(
         strategy=None,
         usage_tracking=True,
         executors=[RADICALExecutor(
                        label        = 'RADICALExecutor',
                        resource     = 'local.localhost',
                        login_method = 'local',
                        project      = '',
                        partition    = '',
                        walltime     = 30,
                        managed      = True,
                        max_tasks    = 1)
                   ])

parsl.load(config)


@python_app
def sleep_mult(delay, x, y, nproc):
    import time
    time.sleep(delay)   # Sleep for 2 seconds
    return x * y


# Launch two apps, which will execute in parallel, since they do not have to
# wait on any futures
res_1 = sleep_mult(1, 2, 4, nproc=1)
res_2 = sleep_mult(1, 4, 8, nproc=1)

print(res_1.result())
print(res_2.result())

# The third app depends on the first two:
#    res_1   res_2     (2 s)
#        \  /
#       res_3          (2 s)
res_3 = sleep_mult(1, res_1.result(), res_2.result(), nproc=1)

# res_3 will be done in ~4s
print(res_3.result())

