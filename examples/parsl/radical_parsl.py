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
                        project = '',
                        partition = '',
                        task_process_type = None,
                        cores_per_task=1,
                        walltime = 30,
                        managed = True,
                        max_tasks = 1)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)

@bash_app
def gen(outputs=[], stdout= '/home/aymen/rand.out',stderr='/home/aymen/rand.err'):
    return 'echo $(( RANDOM % (10 - 5 + 1 ) + 5 )) > {0}'.format(outputs[0])

results = [] 

for i in range(4):
    out_file = "/home/aymen/rand_{0}".format(i)
    results.append(gen(outputs=[out_file]))
    gen()

print ("Job Status: {}".format([r.done() for r in results]))
# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))

outputs = [r.outputs[0] for r in results]
print(outputs)
