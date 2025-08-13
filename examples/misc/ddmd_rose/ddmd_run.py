#!/usr/bin/env python3

import asyncio
from dummy_learner import dummy_workflow
from radical.asyncflow import WorkflowEngine
<<<<<<< HEAD
from radical.asyncflow import RadicalExecutionBackend, ThreadExecutionBackend
from radical.asyncflow import DaskExecutionBackend
=======
from radical.asyncflow import ConcurrentExecutionBackend
from radical.asyncflow import RadicalExecutionBackend
# from radical.asyncflow import DaskExecutionBackend
>>>>>>> 55529eddf (RP debugging only)
#from radical.asyncflow import RadicalExecutionBackend
from concurrent.futures import ThreadPoolExecutor

<<<<<<< HEAD
import argparse
import sys
import os
import json
from pathlib import Path
from rose import LearnerConfig
import random
import shutil

=======
>>>>>>> 55529eddf (RP debugging only)
VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

SIM_CORES = 10
TRAIN_CODE = 6
TOTAL_CORES= SIM_CORES + TRAIN_CODE
<<<<<<< HEAD
#CORES = {'model_train_cores': 1, 'md_sim_cores': TOTAL_CORES - 1}
#BACKEND = RadicalExecutionBackend({'resource': 'local.localhost'})
=======
>>>>>>> 55529eddf (RP debugging only)

RESOURCES = {
            'runtime': 30, 
            'resource': 'local.localhost', 
#            'resource': 'purdue.anvil',
<<<<<<< HEAD
            'cores': TOTAL_CORES
=======
#            'cores': TOTAL_CORES
>>>>>>> 55529eddf (RP debugging only)
        }

raptor_config = {
    "masters": [{
        "ranks": 1,
        "workers": [{
<<<<<<< HEAD
            "ranks": SIM_CORES
=======
            "ranks": 1
>>>>>>> 55529eddf (RP debugging only)
        }]
    }]
}

async def run_ddmd():

<<<<<<< HEAD
    #engine = RadicalExecutionBackend(RESOURCES, raptor_config)
    #engine = DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})
    engine = ThreadExecutionBackend({})
    asyncflow = WorkflowEngine(engine)
    workflow = dummy_workflow(asyncflow=asyncflow, home_dir='DDMD')
    #try:
    await workflow.teach()
    #except Exception as e:
    #    print(f"While teching nn error occurred: {e}")
    #finally:
    workflow.close()
=======
    #engine = await ConcurrentExecutionBackend(ThreadPoolExecutor())
    engine = await RadicalExecutionBackend(RESOURCES, raptor_config)
    #engine = await RadicalExecutionBackend(RESOURCES)
    #engine = await RadicalExecutionBackend({'resource': 'local.localhost'})
    #engine = await DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})

    asyncflow = await WorkflowEngine.create(engine)
    workflow = dummy_workflow(asyncflow=asyncflow)
    try:
        await workflow.teach()
    except Exception as e:
       print(f"While teching nn error occurred: {e}")
    finally:
        workflow.close()
>>>>>>> 55529eddf (RP debugging only)

if __name__ == '__main__':
    asyncio.run(run_ddmd())