#!/usr/bin/env python3

import asyncio
from dummy_learner import dummy_workflow
from radical.asyncflow import WorkflowEngine
from radical.asyncflow import RadicalExecutionBackend, ThreadExecutionBackend
from radical.asyncflow import DaskExecutionBackend
#from radical.asyncflow import RadicalExecutionBackend

import argparse
import sys
import os
import json
from pathlib import Path
from rose import LearnerConfig
import random
import shutil

VAL_SPLIT = 0.2
MIN_TRAIN_SIZE = 1

SIM_CORES = 10
TRAIN_CODE = 6
TOTAL_CORES= SIM_CORES + TRAIN_CODE
#CORES = {'model_train_cores': 1, 'md_sim_cores': TOTAL_CORES - 1}
#BACKEND = RadicalExecutionBackend({'resource': 'local.localhost'})

RESOURCES = {
            'runtime': 30, 
            'resource': 'local.localhost', 
#            'resource': 'purdue.anvil',
            'cores': TOTAL_CORES
        }

raptor_config = {
    "masters": [{
        "ranks": 1,
        "workers": [{
            "ranks": SIM_CORES
        }]
    }]
}

async def run_ddmd():

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

if __name__ == '__main__':
    asyncio.run(run_ddmd())