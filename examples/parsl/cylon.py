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
                        label = 'RADICALExecutor',
                        resource = 'local.localhost',
                        login_method = 'local',
                        project = '',
                        partition = '',
                        walltime = 30,
                        managed = True,
                        max_tasks = 12)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def cylon(comm = None, cpu_processes=2, cpu_process_type=rp.MPI):
    # cpu_processes represents the MPI communicator size (comm)
    import pandas as pd
    from numpy.random import default_rng

    from pycylon.frame import CylonEnv, DataFrame
    from pycylon.net import MPIConfig

    comm = comm

    config = MPIConfig(comm)
    env = CylonEnv(config=config, distributed=True)

    r = 5

    rng = default_rng()
    data1 = rng.integers(0, r, size=(r, 2))
    data2 = rng.integers(0, r, size=(r, 2))

    df1 = DataFrame(pd.DataFrame(data1).add_prefix("col"))
    df2 = DataFrame(pd.DataFrame(data2).add_prefix("col"))

    df3 = df1.merge(df2, on=['col0'], env=env) # dist join
    print(df3)

    env.finalize()


cylon_app = cylon(comm = None,cpu_processes=2, cpu_process_type=rp.MPI)

print(cylon_app.result())
