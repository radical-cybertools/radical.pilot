
Partitions
----------

  - bootstrap_0.sh remains as is, but starts agent.py, the main agent
  - the agent will load bootstrap.1.py. That hosts a partition manager which
    - loads the RM to get node list
    - partitions the node list
    - runs agent.<n> on each partition (formerly agent.0)
    - start an input channel which distributes units to partitions based on
      another scheduling level (default: pull based, option: capacity and
      capability based)
      - TODO: should we introduce a new state?
  - agent.<n> will
    - place bridges, components and sub-agents as configured on its partition
    - pull from unit input channel
    - work on units
    - push done units on it's own output channel (update worker)
    -
        
