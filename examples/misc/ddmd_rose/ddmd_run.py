#!/usr/bin/env python3

#import asyncio
from dummy_learner import dummy_workflow
#from radical.asyncflow import RadicalExecutionBackend

TOTAL_CORES=16
#CORES = {'model_train_cores': 1, 'md_sim_cores': TOTAL_CORES - 1}
#BACKEND = RadicalExecutionBackend({'resource': 'local.localhost'})

RESOURCES = {'runtime': 30, 'resource': 'local.localhost'}
RESOURCES = {
        'resource': 'purdue.anvil',
        'runtime' : 30,
        'cores'   : TOTAL_CORES,
    }

# async def run_ddmd():
#     try:
#         workflow = dummy_workflow(resources=RESOURCES, cores=CORES)
#         await workflow.start()
#     finally:
#         await workflow.close()

def run_ddmd():

    workflow = dummy_workflow(resources=RESOURCES)   #, cores=CORES)
    try:
        workflow.teach()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        workflow.close()

if __name__ == '__main__':
    #asyncio.run(run_ddmd())
    run_ddmd()