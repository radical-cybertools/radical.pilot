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
                        label = 'RADICALExecutorSHELL',
                        resource = 'local.localhost',
                        login_method = 'local',
                        project = '',
                        partition = '',
                        walltime = 30,
                        managed = True,
                        max_tasks = 1),
                      
                      RADICALExecutor(
                        label = 'RADICALExecutorFUNC',
                        resource = 'local.localhost_funcs',
                        login_method = 'local',
                        project = '',
                        partition = '',
                        walltime = 30,
                        managed = True,
                        max_tasks = 1)
                      ],
strategy= None,
usage_tracking=True)

parsl.load(config)


@bash_app(executors=["RADICALExecutorSHELL"])
def generate(nproc=1, ptype= None, outputs=[]):
    return "echo $(( RANDOM )) &> {}".format(outputs[0].filepath)


@python_app(executors=["RADICALExecutorFUNC"])
def mathma(a, nproc):  #python function has no ptype
    import math
    for i in range(10):
        x = math.exp(a*i)
        print (x)

genr_results = []
math_results = []

# We call the generate and store the output in a file

genr_results.append(generate(outputs=[File('/home/aymen/random.txt')]))
# wait for all apps to complete
[r.result() for r in genr_results]

with open('/home/aymen/random.txt', 'r') as f:
     num = f.readlines()
     
math_results.append(mathma(num, nproc=1))

# wait for all apps to complete
[r.result() for r in math_results]

# print each job status, they will now be finished
print ("Job Status: {}".format([r.done() for r in results]))
