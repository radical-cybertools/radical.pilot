import parsl
import radical.pilot as rp
import os 
from parsl.app.app import python_app, bash_app
from parsl import File

from parsl.config import Config
from parsl.channels import SSHChannel
from parsl.launchers import SrunLauncher
from parsl.providers import SlurmProvider
from parsl.addresses import address_by_query
from parsl.executors import HighThroughputExecutor
from parsl.executors.radical_executor.radical_executor  import RADICALExecutor

parsl.set_stream_logger()
os.environ['RADICAL_PILOT_DBURL'] = ''
config = Config(
         executors=[RADICALExecutor(
                        label = 'RADICALExecutor',
                        resource = 'local.localhost_funcs', #'local.localhost',
                        login_method = 'local',
                        tasks =list(),
                        task_process_type = rp.FUNC,
                        cores_per_task=1,
                        managed = True,
                        max_tasks = 1)
                        ],
strategy= None,
usage_tracking=True)

parsl.load(config)


@python_app
def timer():

    x = time.time()
    return x

"""
@python_app
def add(a: int, b: int):
    return a + b

"""
r = timer()

print(r)

parsl.dfk().cleanup()
