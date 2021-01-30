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
                   'cores'         : 16
                  }
        pdesc = rp.ComputePilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        umgr  = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        cuds = list()
        for i in range(1):

            cud = rp.ComputeUnitDescription()
            cud.executable       = '/bin/sh'
            cud.arguments        = ['-c', 'echo "out $RP_RANK: `date`"; '
                                          'env | grep RP_ | sort > $RP_RANK.env']
            cud.cpu_processes    = 4
            cud.gpu_processes    = 1
            cud.cpu_process_type = rp.MPI
            cud.pre_launch       = ['echo   pre_launch',
                                    'export RP_PRE_LAUNCH=True']
            cud.pre_exec         = ['echo "pre exec $RP_RANK: `date`"',
                                    'export RP_PRE_EXEC=True']
            cud.pre_rank         = {'0': ['export RP_PRE_RANK_0=True',
                                          'echo pre_rank 0:$RP_RANK: `date`',
                                          'sleep 2',
                                          'echo pre_rank 0:$RP_RANK: `date`',
                                         ],
                                    '3': ['export RP_PRE_RANK_3=True',
                                          'echo pre_rank 3:$RP_RANK: `date`',
                                          'sleep 5',
                                          'echo pre_rank 3:$RP_RANK: `date`',
                                          ]}
            cud.post_rank        = ['echo post_rank $RP_RANK']
            cud.post_exec        = ['echo post_exec $RP_RANK']
            cud.post_launch      = ['echo post_launch']
            cud.environment      = {'FOO_BAR': 'foo_bar'}
            cud.named_env        = {'name': 'foo_env',
                                    'cmds': ['export RP_NAMED_ENV=True']}
            cuds.append(cud)

        umgr.submit_units(cuds)
        umgr.wait_units()
        session.close(download=True)

    except:
        session.close(download=False)
        raise


# ------------------------------------------------------------------------------

