*********
Profiling
*********

.. note:: This section is for developers, and should be disregarded for production
          runs and 'normal' users in general.

RADICAL-Pilot allows to tweak the pilot process behavior in many details, and
specifically allows to artificially increase the load on individual
components, for the purpose of more detailed profiling, and identification of
bottlenecks. With that background, a pilot description supports an additional
attribute `_config`, which accepts a dict of the following structure:

.. code-block:: python

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 5 # minutes
        pdesc.cores    = 8
        pdesc.cleanup  = False
        pdesc._config  = {'number_of_workers' : {'StageinWorker'   :  1,
                                                 'ExecWorker'      :  2,
                                                 'StageoutWorker'  :  1,
                                                 'UpdateWorker'    :  1},
                          'blowup_factor'     : {'Agent'           :  1,
                                                 'stagein_queue'   :  1,
                                                 'StageinWorker'   :  1,
                                                 'schedule_queue'  :  1,
                                                 'Scheduler'       :  1,
                                                 'execution_queue' : 10,
                                                 'ExecWorker'      :  1,
                                                 'watch_queue'     :  1,
                                                 'Watcher'         :  1,
                                                 'stageout_queue'  :  1,
                                                 'StageoutWorker'  :  1,
                                                 'update_queue'    :  1,
                                                 'UpdateWorker'    :  1},
                          'drop_clones'       : {'Agent'           :  1,
                                                 'stagein_queue'   :  1,
                                                 'StageinWorker'   :  1,
                                                 'schedule_queue'  :  1,
                                                 'Scheduler'       :  1,
                                                 'execution_queue' :  1,
                                                 'ExecWorker'      :  0,
                                                 'watch_queue'     :  0,
                                                 'Watcher'         :  0,
                                                 'stageout_queue'  :  1,
                                                 'StageoutWorker'  :  1,
                                                 'update_queue'    :  1,
                                                 'UpdateWorker'    :  1}}

That configuration tunes the concurrency of some of the pilot components (here
we use two `ExecWorker` instances to spawn units.  Further, we request that
the number of compute units handled by the `ExecWorker` is 'blown up'
(multiplied) by 10.  This will created 9 near-identical units for every unit
which enters that component, and thus the load increases on that specific
component, but not on any of the previous ones.  Finally, we instruct all
components but the `ExecWorker`, `watch_queue` and `Watcher` to drop the
clones again, so that later components won't see those clones either.  We thus
strain only a specific part of the pilot.

Setting these parameters requires some understanding of the pilot
architecture. While in general the application semantics remains unaltered,
these parameters do significantly alter resource consumption.  Also, there do
exist invalid combinations which will cause the agent to fail, specifically it
will usually be invalid to push updates of cloned units to the client module
(via MongoDB).

The pilot profiling (as stored in `agent.prof` in the pilot sandbox) will
contain timings for the cloned units.  The unit IDs will be based upon the
original unit IDs, but have an appendix `.clone.0001` etc., depending on the
value of the respective blowup factor.  In general, only one of the
blowup-factors should be larger than one (otherwise the number of units will
grow exponentially, which is probably not what you want).