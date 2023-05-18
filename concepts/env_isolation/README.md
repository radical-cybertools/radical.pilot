

The purpose of this code is to ensure a stable environment for RP tasks,
which ideally is (as) identical (as possible) to the environment the pilot
is originally placed in.

API / config changes

   - a task has two sets of pre_exec directives:
     - `pre_exec_env`: commands to prepare the environment
       - eg., export, module load, source, ...
       - runs on the agent node
       - can be cached: identical command sets are run only once, env is then
         reused for other tasks
     - `pre_exec_cmd`: any action needed to run the task
       - runs on the compute node
       - will block all task resources apart from those assigned to rank 0
       - runs in the task env (but not in the MPI env)
     - backward compatibility: `pre_exec` is interpreted as `pre_exec_env`
   - each LM has it's individual env settings (module load etc)
     - any number of LMs can be configured for an agent
     - TODO: clarify how a LM is selected for a task


Naming:

  - *.dump: environment dumps (`env > env.dump`)
  - *.sh  : environment setup (unset and export command lines, can be sourced)


Procedural changes:

   - capture the original environment (`env.boot.dump`)
   - prepare the agent VE and env, and start agent, sub-agent, agent-executor
   - based on env.boot, agent prepares new env *per launch method* (`env.lm_x.sh`)
   - when launching a task
     - prepare and capture task env based on `env.boot.dump` (`env.task.sh`)
       and cache it
     - prepare and run launch method script (using `env.lm_x.sh`)
     - prepare and run task wrapper script
     - prepare and run task ranks
       - load env.task.sh
       - if rank == 0:
         - unset any env changes caused by calling the LM (MPI ranks etc)
         - run pre_exec_cmd
         - restore `env.task.sh`
       - run task executable (all ranks)

