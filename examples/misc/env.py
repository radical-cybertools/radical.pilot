#!/usr/bin/env python3

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    session = rp.Session()

    try:
        pmgr    = rp.PilotManager(session=session)
        pd_init = {'resource': 'local.localhost',
                   'runtime' : 10,
                   'cores'   : 32,
                   'gpus'    : 4
                  }
        pdesc = rp.PilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        umgr  = rp.TaskManager(session=session)
        umgr.add_pilots(pilot)

        tds = list()
        for i in range(1):

            td = rp.TaskDescription()
            td.executable    = '/bin/sh'
            td.arguments     = ['-c', 'echo "out $RP_RANK: `date`"; '
                                      'env | grep RP_ | sort > $RP_RANK.env']
            td.ranks         = 4
            td.gpus_per_rank = 1
            td.pre_launch    = ['echo   pre_launch',
                                'export RP_PRE_LAUNCH=True']
            td.pre_exec      = ['echo "pre exec $RP_RANK: `date`"',
                                'export RP_PRE_EXEC=True',
                                {'0': ['export RP_PRE_RANK_0=True',
                                       'echo pre_exec 0:$RP_RANK: `date`',
                                       'sleep 2',
                                       'echo pre_exec 0:$RP_RANK: `date`',
                                       ],
                                 '3': ['export RP_PRE_RANK_3=True',
                                       'echo pre_exec 3:$RP_RANK: `date`',
                                       'sleep 5',
                                       'echo pre_exec 3:$RP_RANK: `date`',
                                       ]}]
            td.post_exec     = ['echo "post exec $RP_RANK: `date`"',
                                'export RP_POST_EXEC=True',
                                {'1': ['export RP_POST_RANK_1=True',
                                       'echo post_exec 1:$RP_RANK: `date`',
                                       'sleep 2',
                                       'echo post_exec 1:$RP_RANK: `date`',
                                       ],
                                 '3': ['export RP_POST_RANK_3=True',
                                       'echo post_exec 3:$RP_RANK: `date`',
                                       'sleep 5',
                                       'echo post_exec 3:$RP_RANK: `date`',
                                       ]}]
            td.post_launch   = ['echo post_launch']
            td.environment   = {'FOO_BAR': 'foo_bar'}
          # td.named_env     = {'name': 'foo_env',
          #                     'cmds': ['export RP_NAMED_ENV=True']}
            tds.append(td)

        umgr.submit_tasks(tds)
        umgr.wait_tasks()
        session.close(download=True)

    except:
        session.close(download=False)
        raise


# ------------------------------------------------------------------------------

