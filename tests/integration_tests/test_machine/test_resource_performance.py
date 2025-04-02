#!/usr/bin/env python3

__author__    = 'RADICAL-Cybertools Team'
__email__     = 'info@radical-cybertools.org'
__copyright__ = 'Copyright 2025, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import argparse
import os
import random
import sys

import urllib.request
import radical.pilot as rp

NODES = 1
CORES_PER_NODE = 128
GPUS_PER_NODE = 2

TASK_GROUPS = 2
# tasks with 1 CPU core
CPU_TASKS_PER_GROUP = 1 * NODES
# tasks with 1 CPU core and 1 GPU
GPU_TASKS_PER_GROUP = GPUS_PER_NODE * NODES


def get_args():
    parser = argparse.ArgumentParser(usage='test_resource.py [<options>]')
    parser.add_argument('-r', '--resource', dest='resource', type=str, required=True)
    parser.add_argument('-p', '--project', default='', dest='project', type=str, required=False)
    parser.add_argument('-g', '--use_gpus', default=False, dest='use_gpus', type=bool, required=False)

    return parser.parse_args(sys.argv[1:])


def check_test_executable():
    
    if not os.path.exists('radical-pilot-hello.sh'):
        
        url = 'https://github.com/radical-cybertools/radical.pilot/blob/devel/bin/radical-pilot-hello.sh'
        filename = url.split('/')[-1]
        file_path = os.path.join('.', filename)

        # Download the file
        print(f"Downloading {url} to {file_path}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Download complete! File saved to {file_path}")


def main():
    
    session = rp.Session()
    try:
        args = get_args()
        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)
        
        pilot = {'resource' : args.resource,
                 'cores': 64, 'gpus': 4, 'runtime': 60}

        if args.project:
            pilot['project'] = args.project

        pilot = pmgr.submit_pilots(rp.PilotDescription(pilot))
        tmgr.add_pilots(pilot)

        tds = []
        check_test_executable()
        for _ in range(TASK_GROUPS):
            for _ in range(CPU_TASKS_PER_GROUP):
                tds.append(rp.TaskDescription({
                    # RP test executable:
                    'executable'    : 'radical-pilot-hello.sh',
                    'arguments'     : [random.randint(30, 50)],
                    'named_env'     : 'rp'
                }))
            
            if args.use_gpus:
                for _ in range(GPU_TASKS_PER_GROUP):
                    tds.append(rp.TaskDescription({
                        'gpus_per_rank' : 1,
                        # RP test executable:
                        'executable'    : 'radical-pilot-hello.sh',
                        'arguments'     : [random.randint(30, 50)],
                        'named_env'     : 'rp'
                    }))

        tmgr.submit_tasks(tds)
        tmgr.wait_tasks()
    finally:
        session.close(download=True)


if __name__ == '__main__':

    os.environ['RADICAL_PROFILE'] = 'TRUE'
    # for test purposes
    os.environ['RADICAL_LOG_LVL'] = 'DEBUG'
    os.environ['RADICAL_REPORT']  = 'TRUE'

    main()
