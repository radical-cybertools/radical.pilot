
import os
import copy

import subprocess    as sp

import radical.utils as ru


eiu = __import__('00_env_isolation_utils')
eie = __import__('03_env_isolation_exec')

env_read = eiu.env_read
env_prep = eiu.env_prep


# The executor process which will further change environment and then pass on to
# the task's launch method in it's own LM specific environment

# read and parse the environment as originally stored by the bootstrapper,
# ensure valid keys.  This is the reference env we need to provide to the
# application.
env_boot = env_read('./env.boot.env')

# each launch method may require it's own environment.  Specifically launcher
# environments may conflict, if for example one launcher needs OpenMPI loaded,
# and another launcher MPICH.  To prepare the launcher envs, run a set of
# pre_exec's for each of them.  We base the launcher environment on the
# *bootstrapper* env, not on the agent env.  So we could, for example, use
# a different set of modules for the launcher script than for the agent.  Since
# the launcher is a shell script (in most cases), we don't need to share the
# python environment).

# ------------------------------------------------------------------------------
#
def executor():

    os.environ['RP_TEST']      = 'EXEC'
    os.environ['RP_TEST_EXEC'] = 'True'

    env_lm = {
            'OpenMPI' : env_prep(base=env_boot,
                                 remove={},  # ?
                                 pre_exec_env=['export RP_TEST=LM_1',
                                               'export RP_TEST_LAUNCH=OpenMPI'
                                              # module load openmpi
                                              ],
                                 tgt="./env.lm_1.sh"),

            'MPICH'   : env_prep(base=env_boot,
                                 remove={},  # ?
                                 pre_exec_env=['export RP_TEST=LM_2',
                                               'export RP_TEST_LAUNCH=MPICH'
                                              # module load mpich
                                              ],
                                 tgt="./env.lm_2.sh")
    }


    # The type of launch method to use is hardcoded in this example code, but
    # would be switched by task type really.  It is used to pick the respective
    # launcher environment.
    lm_type = 'OpenMPI'
    lm_env  = env_lm[lm_type]

    # The task may specify it's own env preparation commands (pre_exec_env).
    # The executor will prepare that env, too (but caches it for identical
    # tasks), and dynamically write an `env.task.sh` into the task sandbox which
    # is then used by the task wrapper  (caching goes into env_prep).
    env_task = env_prep(base=env_boot,
                        remove=lm_env,
                        pre_exec_env=['export RP_TEST=PRE',
                                      'export RP_TEST_PRE=True',
                                     # module load gromacs
                                     ],
                        tgt='./env.task.sh')

    # Note that both, the task launcher and the task wrapper scripts would be
    # created here on the fly - we hardcode them in this example.
    proc = sp.Popen('./04_env_isolation_launcher.sh', env=env_lm[lm_type])

    # `proc` would now be passed to the watcher thread and collected
    # asynchronously - here we wait it synchronously.
    proc.wait()


# ------------------------------------------------------------------------------

