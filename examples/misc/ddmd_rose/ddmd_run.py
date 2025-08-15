#!/usr/bin/env python3

import asyncio
from dummy_learner import DummyWorkflow
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import ConcurrentExecutionBackend
from radical.asyncflow import RadicalExecutionBackend
# from radical.asyncflow import DaskExecutionBackend
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

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
    engine = await ConcurrentExecutionBackend(ProcessPoolExecutor())
    #engine = await RadicalExecutionBackend(RESOURCES, raptor_config)
    #engine = await RadicalExecutionBackend(RESOURCES)
    #engine = await RadicalExecutionBackend({'resource': 'local.localhost'})
    #engine = await DaskExecutionBackend({'n_workers': 2, 'threads_per_worker': 1})

    # Create the async workflow engine
    asyncflow = await WorkflowEngine.create(engine)
    
    # Initialize the workflow
    workflow = DummyWorkflow(asyncflow=asyncflow)
    
    try:
        # Run the workflow
        await workflow.teach()
    except Exception as e:
        print(f"An error occurred during teaching: {e}")
    finally:
        # Ensure cleanup regardless of errors
        workflow.close()

if __name__ == '__main__':
    asyncio.run(run_ddmd())