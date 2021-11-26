#!/usr/bin/env python3


import radical.pilot as rp

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    session = rp.Session()

    try:
        pmgr    = rp.PilotManager(session=session)
        pd_init = {'resource'      : 'local.debug',
                   'runtime'       : 10,
                   'cores'         : 32
                  }
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        umgr  = rp.TaskManager(session=session)
        umgr.add_pilots(pilot)

        tds = list()
        for i in range(1):

            td = rp.TaskDescription()
            td.executable       = '/bin/sh'
            td.arguments        = ['-c', 'echo "out $RP_RANK: `date`"; '
                                         'env | grep RP_ | sort > $RP_RANK.env']
            td.cpu_processes    = 4
            td.gpu_processes    = 1
            td.cpu_process_type = rp.MPI
            td.pre_launch       = ['echo   pre_launch',
                                   'export RP_PRE_LAUNCH=True']
            td.pre_exec         = ['echo "pre exec $RP_RANK: `date`"',
                                   'export RP_PRE_EXEC=True']
            td.pre_rank         = {'0': ['export RP_PRE_RANK_0=True',
                                         'echo pre_rank 0:$RP_RANK: `date`',
                                         'sleep 2',
                                         'echo pre_rank 0:$RP_RANK: `date`',
                                        ],
                                   '3': ['export RP_PRE_RANK_3=True',
                                         'echo pre_rank 3:$RP_RANK: `date`',
                                         'sleep 5',
                                         'echo pre_rank 3:$RP_RANK: `date`',
                                         ]}
            td.post_rank        = ['echo post_rank $RP_RANK']
            td.post_exec        = ['echo post_exec $RP_RANK']
            td.post_launch      = ['echo post_launch']
            td.environment      = {'FOO_BAR': 'foo_bar'}
            td.named_env        = {'name': 'foo_env',
                                   'cmds': ['export RP_NAMED_ENV=True']}
            tds.append(td)

        umgr.submit_tasks(tds)
        umgr.wait_tasks()
        session.close(download=True)

    except:
        session.close(download=False)
        raise


# ------------------------------------------------------------------------------

