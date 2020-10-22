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
                        project = ''
                        resource = 'xsede.comet_ssh',
                        login_method = 'gsissh',
                        partition = ''
                        walltime  = 30
                        task_process_type = None,
                        cores_per_task=1,
                        managed = True,
                        max_tasks = 1) # total number of tasks * core per task
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

'''
@bash_app
def gen(outputs=[], stdout= '/home/aymen/rand.out',stderr='/home/aymen/rand.err'):
    return 'echo $(( RANDOM % (10 - 5 + 1 ) + 5 ))'

for i in range(8):
    zz = gen()
'''

@bash_app
def stress(outputs=[], stdout= '/home/aymen/rand.out',stderr='/home/aymen/rand.err'):
    return '/home/aymen/stress-ng/stress-ng --cpu 1 --timeout 300'

for i in range(8):
    zz = stress()