.. _chapter_supported:

=======================
Supported HPC Platforms
=======================

RADICAL-Pilot (RP) supports a variety of high performance computing (HPC)
platforms, across the Department of Energy and the National Science Foundation.
RP utilizes json configuration files to store the parameters specific to each
platform. See the `configuration tutorial <https://radicalpilot.readthedocs.io/en/stable/tutorials/configuration.html#Platform-description>`_
for more information about how to write a configuration file for a new platform.
You can also see the current configuration file for each of the supported
platforms in the tables below.

Department of Energy (DOE) HPC Platforms
----------------------------------------

.. csv-table:: 
   :header: "Name", "FQDN", "Launch Method", "Configuration File"
   :widths: auto

   "`Andes     <https://docs.olcf.ornl.gov/systems/andes_user_guide.html>`_",            "andes.olcf.ornl.gov",       "``srun``",                                 "`resource_ornl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ornl.json>`_"
   "`Aurora    <https://docs.alcf.anl.gov/aurora/getting-started-on-aurora/>`_",         "aurora.alcf.anl.gov",       "``mpiexec``",                              "`resource_anl.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_anl.json>`_"
   "`Frontier  <https://docs.olcf.ornl.gov/systems/frontier_user_guide.html>`_",         "frontier.olcf.ornl.gov",    "``srun``",                                 "`resource_ornl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ornl.json>`_"
   "`Lassen    <https://hpc.llnl.gov/hardware/compute-platforms/lassen>`_",              "lassen.llnl.gov",           "``jsrun``",                                "`resource_llnl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_llnl.json>`_"
   "`Polaris   <https://docs.alcf.anl.gov/polaris/getting-started/>`_",                  "polaris.alcf.anl.gov",      "``mpiexec``",                              "`resource_anl.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_anl.json>`_"

National Science Foundation (NSF) HPC Platforms
-----------------------------------------------

.. csv-table:: 
   :header: "Name", "FQDN", "Launch Method", "Configuration File"
   :widths: auto

   "`Bridges2  <https://www.psc.edu/resources/bridges-2/user-guide/>`_",                 "bridges2.psc.edu",          "``srun``",                                 "`resource_access.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_access.json>`_"
   "`Delta     <https://docs.ncsa.illinois.edu/systems/delta/en/latest/>`_",             "delta.ncsa.illinois.edu",   "``srun``",                                 "`resource_ncsa.json     <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ncsa.json>`_"
   "`Expanse   <https://www.sdsc.edu/support/user_guides/expanse.html>`_",               "expanse.sdsc.edu",          "``srun``, ``mpirun``",                     "`resource_access.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_access.json>`_"
   "`Frontera  <https://docs.tacc.utexas.edu/hpc/frontera/>`_",                          "frontera.tacc.utexas.edu",  "``mpirun``, ``srun``, ``prte``",           "`resource_tacc.json     <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_tacc.json>`_"
   "`Stampede3 <https://docs.tacc.utexas.edu/hpc/stampede3/>`_",                         "stampede3.tacc.utexas.edu", "``ibrun``, ``mpirun``, ``srun``",          "`resource_access.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_access.json>`_"

Campus HPC Platforms
--------------------

.. csv-table::
   :header: "Name", "FQDN", "Launch Method", "Configuration File"
   :widths: auto

   "`Amarel    <https://sites.google.com/view/cluster-user-guide>`_",                    "amarel.rutgers.edu",        "``srun``",             "`resource_rutgers.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_rutgers.json>`_"
   "`Tiger     <https://researchcomputing.princeton.edu/systems/tiger>`_",               "tiger.princeton.edu",       "``srun``",             "`resource_princeton.json <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_princeton.json>`_"
   "`Traverse  <https://researchcomputing.princeton.edu/systems/traverse>`_",            "traverse.princeton.edu",    "``srun``, ``mpirun``", "`resource_princeton.json <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_princeton.json>`_"
   "`Rivanna   <https://www.rc.virginia.edu/userinfo/rivanna/overview/>`_",              "rivanna.hpc.virginia.edu",  "``mpirun``",           "`resource_uva.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_uva.json>`_"


Guides
------

.. toctree::
   :maxdepth: 2
   :glob:

   supported/*
