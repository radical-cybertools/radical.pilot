.. _chapter_supported:

=======================
Supported HPC Platforms
=======================

RADICAL-Pilot (RP) supports a variety of high performance computing (HPC) platforms, across the Department of Energy and the National Science Foundation.
RP utilizes json configuration files to store the parameters specific to each platform. See the configuration `tutorial https://radicalpilot.readthedocs.io/en/stable/tutorials/configuration.html#Platform-description`_ for more information about how to write a configuration file for a new platform. You can also see the current configuration file for each of the supported platforms in the tables below.

Department of Energy (DOE) HPC Platforms
----------------------------------------

.. csv-table:: 
   :header: "Name", "FQDN", "Launch Method", "Configuration File"
   :widths: auto

   "`Andes     <https://docs.olcf.ornl.gov/systems/andes_user_guide.html>`_",            "andes.olcf.ornl.gov",       "``srun``",                                 "`resource_ornl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ornl.json>`_"
   "`Arcticus  <https://www.jlse.anl.gov/hardware-under-development/>`_",                "arcticus.alcf.anl.gov",     "``mpirun``, ``ssh``",                      "`resource_anl.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_anl.json>`_"
   "`Cheyenne  <https://kb.ucar.edu/display/RC/Quick+start+on+Cheyenne>`_",              "cheyenne.ucar.edu",         "``fork``, ``mpirun``, ``mpiexec_mpt``",    "`resource_ncar.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ncar.json>`_"
   "`Crusher   <https://docs.olcf.ornl.gov/systems/crusher_quick_start_guide.html>`_",   "crusher.olcf.ornl.gov",     "``srun``",                                 "`resource_ornl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ornl.json>`_"
   "`Frontier  <https://docs.olcf.ornl.gov/systems/frontier_user_guide.html>`_",         "frontier.olcf.ornl.gov",    "``srun``",                                 "`resource_ornl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ornl.json>`_"
   "`Lassen    <https://hpc.llnl.gov/hardware/compute-platforms/lassen>`_",              "lassen.llnl.gov",           "``fork``, ``jsrun``",                      "`resource_anl.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_anl.json>`_"
   "`Polaris   <https://docs.alcf.anl.gov/polaris/getting-started/>`_",                  "polaris.alcf.anl.gov",      "``mpiexec``",                              "`resource_anl.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_anl.json>`_"
   "`Summit    <https://docs.olcf.ornl.gov/systems/summit_user_guide.html>`_",           "summit.olcf.ornl.gov",      "``jsrun``, ``mpirun``, ``ssh``, ``prte``", "`resource_ornl.json      <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_ornl.json>`_"
   "`Theta     <https://docs.alcf.anl.gov/theta/hardware-overview/machine-overview/>`_", "theta.alcf.anl.gov",        "``aprun``, ``mpirun``, ``ssh``",           "`resource_anl.json       <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_anl.json>`_"

National Science Foundation (NSF) HPC Platforms
-----------------------------------------------

.. csv-table:: 
   :header: "Name", "FQDN", "Launch Method", "Configuration File"
   :widths: auto

   "`Bridges2  <https://www.psc.edu/resources/bridges-2/user-guide-2-2/>`_",             "bridges2.psc.edu",          "``mpirun``",                               "`resource_access.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_access.json>`_"
   "`Expanse   <https://www.sdsc.edu/support/user_guides/expanse.html>`_",               "login.expanse.sdsc.edu",    "``mpirun``",                               "`resource_access.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_access.json>`_"
   "`Frontera  <https://frontera-portal.tacc.utexas.edu/user-guide/>`_",                 "frontera.tacc.utexas.edu",  "``mpirun``, ``ssh``, ``srun``, ``prte``",  "`resource_tacc.json     <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_tacc.json>`_"
   "`Stampede2 <https://docs.tacc.utexas.edu/hpc/stampede2/>`_",                         "stampede2.tacc.utexas.edu", "``fork``, ``ibrun``, ``mpirun``, ``ssh``", "`resource_access.json   <https://github.com/radical-cybertools/radical.pilot/blob/devel/src/radical/pilot/configs/resource_access.json>`_"

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
