

.. _chapter_resources:

List of Pre-Configured Resources
================================

sierra
------

The FutureGrid 'sierra' cluster (https://futuregrid.github.io/manual/hardware.html).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

alamo
-----

The FutureGrid 'alamo' cluster (https://futuregrid.github.io/manual/hardware.html).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               short
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

hotel
-----

The FutureGrid 'hotel' cluster (https://futuregrid.github.io/manual/hardware.html).

.. note::  Due to a broken MPI installation, 'hotel' is currently not usable as agents won't start up / run properly.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

india
-----

The FutureGrid 'india' cluster (https://futuregrid.github.io/manual/hardware.html).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

tutorial
--------

Our private tutorial VM on EC2

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, local``

:download:`Raw Configuration file: radical.json <../../src/radical/pilot/configs/radical.json>`

davinci
-------

The DAVinCI Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+DAVinCI).

.. note::  DAVinCI compute nodes have 12 or 16 processor cores per node.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               parallel
``sandbox``             $SHARED_SCRATCH/$USER
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: rice.json <../../src/radical/pilot/configs/rice.json>`

biou
----

The Blue BioU Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+Blue+BioU).

.. note::  Blue BioU compute nodes have 32 processor cores per node.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               serial
``sandbox``             $SHARED_SCRATCH/$USER
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: rice.json <../../src/radical/pilot/configs/rice.json>`

localhost
---------

Your local machine.

.. note::  To use the ssh schema, make sure that ssh access to localhost is enabled.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               None
``sandbox``             $HOME
``access_schema``       local
================== ============================

Available schemas: ``local, ssh``

:download:`Raw Configuration file: local.json <../../src/radical/pilot/configs/local.json>`

yellowstone
-----------

The Yellowstone IBM iDataPlex cluster at UCAR (https://www2.cisl.ucar.edu/resources/yellowstone).

.. note::  We only support one concurrent CU per node currently.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               premium
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: ncar.json <../../src/radical/pilot/configs/ncar.json>`

fs2
---

The Distributed ASCI Supercomputer 4 (http://www.cs.vu.nl/das4/).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               all.q
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: das4.json <../../src/radical/pilot/configs/das4.json>`

bigred2
-------

Indiana University's HPC cluster (https://kb.iu.edu/d/bcqt).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               None
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: iu.json <../../src/radical/pilot/configs/iu.json>`

quarry
------

The Quarry Linux cluster at Indiana University (https://kb.iu.edu/d/avkx).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               None
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: iu.json <../../src/radical/pilot/configs/iu.json>`

archer
------

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               standard
``sandbox``             /work/`id -gn`/`id -gn`/$USER
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: epsrc.json <../../src/radical/pilot/configs/epsrc.json>`

hopper
------

The Nersc Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

.. note::  In a fresh virtualenv, run 'easy_install pip==1.2.1' to avoid ssl errors.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               debug
``sandbox``             /scratch/scratchdirs/$USER
``access_schema``       ssh
================== ============================

Available schemas: ``ssh``

:download:`Raw Configuration file: nersc.json <../../src/radical/pilot/configs/nersc.json>`

supermuc
--------

The SuperMUC petascale HPC cluster at LRZ, Munich (http://www.lrz.de/services/compute/supermuc/).

.. note::  Default authentication to SuperMUC uses X509 and is firewalled, make sure you can gsissh into the machine from your registered IP address. Because of outgoing traffic restrictions your MongoDB needs to run on a port in the range 20000 to 25000.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               test
``sandbox``             $HOME
``access_schema``       gsissh
================== ============================

Available schemas: ``gsissh, ssh``

:download:`Raw Configuration file: lrz.json <../../src/radical/pilot/configs/lrz.json>`

lonestar
--------

The XSEDE 'Lonestar' cluster at TACC (https://www.tacc.utexas.edu/resources/hpc/lonestar).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

stampede
--------

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $WORK
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

gordon
------

The XSEDE 'Gordon' cluster at SDSC (http://www.sdsc.edu/us/resources/gordon/).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

trestles
--------

The XSEDE 'Trestles' cluster at SDSC (http://www.sdsc.edu/us/resources/trestles/).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

blacklight
----------

The XSEDE 'Blacklight' cluster at PSC (https://www.psc.edu/index.php/computing-resources/blacklight).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
``access_schema``       ssh
================== ============================

Available schemas: ``ssh, gsissh``

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

