#!/usr/bin/env python3

import asyncio
from dummy_learner import dummy_workflow
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ConcurrentExecutionBackend
from radical.asyncflow import RadicalExecutionBackend
# from radical.asyncflow import DaskExecutionBackend
#from radical.asyncflow import RadicalExecutionBackend
from concurrent.futures import ThreadPoolExecutor

VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

SIM_CORES = 10
TRAIN_CODE = 6
TOTAL_CORES= SIM_CORES + TRAIN_CODE

RESOURCES = {
            'runtime': 30, 
            'resource': 'local.localhost', 
#            'resource': 'purdue.anvil',
#            'cores': TOTAL_CORES
        }

raptor_config = {
    "masters": [{
        "ranks": 1,
        "workers": [{
            "ranks": 1
        }]
    }]
}

async def run_ddmd():

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

if __name__ == '__main__':
    asyncio.run(run_ddmd())