#!/usr/bin/env python3

import re
import os
import sys
import copy
import pprint

import multiprocessing as mp
import radical.utils   as ru

# # fuck python
# from .00_env_isolation_utils import env_read, env_diff
# from .03_env_isolation_exec  import executor

eiu = __import__('00_env_isolation_utils')
eie = __import__('03_env_isolation_exec')

env_read = eiu.env_read
env_diff = eiu.env_diff
executor = eie.executor

# ------------------------------------------------------------------------------
#
# the agent may change the environment further
#
os.environ['RP_TEST']       = 'AGENT'
os.environ['RP_TEST_AGENT'] = 'True'

# run the executor which will start the task wrapper script.  The executor here
# inherits the agent environment.
p = mp.Process(target=executor)
p.start()
p.join()


# at this point the task has been completed, and will have dumped the
# environment it encountered into `./env.check`.  We read that env and
# compare it to the original env
env_boot  = env_read('./env.boot.env')
env_check = env_read('./env.check.env')

# # some debug print for what changed in the env for the task
# only_boot, only_check, changed = env_diff(env_boot, env_check)
#
# print('------------- only env_boot')
# pprint.pprint(only_boot)
# print('------------- only env_check')
# pprint.pprint(only_check)
# print('------------- changed')
# pprint.pprint(changed)




