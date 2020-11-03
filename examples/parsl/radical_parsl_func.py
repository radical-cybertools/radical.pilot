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
                        resource = 'local.localhost_funcs', #'local.localhost_funcs',
                        login_method = 'local',
                        task_process_type = 'rp.FUNC',
                        project = '',
                        partition = '',
                        cores_per_task=1,
                        walltime = 30,
                        managed = True,
                        max_tasks = 1)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@python_app
def report_time(outputs=[],stdout= '/home/aymen/report_time.out',stderr='/home/aymen/report_time.err'):
    import time
    time.time()

zz = report_time()
