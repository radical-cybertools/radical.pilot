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
                        project = '',
                        resource = 'xsede.comet_ssh',
                        login_method = 'gsissh',
                        partition = 'debug',
                        walltime  = 30,
                        managed = True,
                        max_tasks = 8) # total number of tasks * core per task
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
def stress(exe='',nproc=1, ptype= None,outputs=[], stdout= '', stderr=''):
    return '/home/aymen/stress-ng/stress-ng --cpu 1 --timeout 300 > {0}'.format(outputs[0])


results = []

for i in range(8):
    out_file = "/home/aymen/stress_{0}".format(i)
    results.append(stress(outputs=[out_file]))
    stress()


print ("Job Status: {}".format([r.done() for r in results]))
# wait for all apps to complete
[r.result() for r in results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))

outputs = [r.outputs[0] for r in results]
print(outputs)
