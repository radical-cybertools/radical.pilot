#!/usr/bin/env python3

import asyncio
from ddmd_workflow1 import DDMD_manager
from radical.asyncflow import RadicalExecutionBackend

TOTAL_CORES=16
CORES = {'model_train_cores': 1, 'ff_train_cores': 1, 'md_sim_cores': TOTAL_CORES - 2}
BACKEND = RadicalExecutionBackend({'resource': 'local.localhost'})

async def run_ddmd():

    try:
        ddmd_manager = DDMD_manager(execution_backend=BACKEND, cores=CORES)
        await ddmd_manager.start()
    finally:
        await ddmd_manager.close()


if __name__ == '__main__':
    asyncio.run(run_ddmd())