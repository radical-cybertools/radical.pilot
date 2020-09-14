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
                        resource = 'local.localhost', #'local.localhost_funcs',
                        login_method = 'local',
                        task_process_type = None,
                        cores_per_task=1,
                        managed = True,
                        max_tasks = 1)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)


@bash_app
def generate(outputs=[], 
             stdout= '/home/aymen/stress.out',stderr='/home/aymen/stress.err'):

    return "echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) &> {o}".format(o=outputs[0])

parsl.dfk().cleanup()
