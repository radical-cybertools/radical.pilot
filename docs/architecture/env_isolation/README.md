
 API / config changes

   - a task has two sets of pre_exec:
     - pre_exec_env: commands to prepare the environment
       - eg., export, module load, source, ...
       - must be fast
       - cached (identical command sets are run only once, env is then reused)
     - pre_exec_action: any action needed to run the task
       - runs on the compute node
       - will block all task resources apart from one core (rank really)
       - runs in the task env
   - each LM has it's individual env settings (module load etc)
     - any number of LMs can be configured for an agent
     - clarify how a LM is selected for a task


Procedural changes:

   - capture the original environment (env.boot)
   - prepare the agent VE and env, and start agent, sub-agent, agent-executor
   - based on env.boot, agent prepares new env *per launch method* (env.lm_x)
   - when launching a task
     - prepare and capture task env (env.task) and cache it
     - prepare and run launch method script
     - prepare and run task wrapper script
     - prepare and run task ranks
   - the launch method script
     - load env.lm_x
     - call launch method
   - the task wrapper script
     - load env.task
     - if rank == 1:
       - block all other ranks
       - run pre_exec_action
       - release block
     - run task executable
