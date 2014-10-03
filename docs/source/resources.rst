fs2.das4.science.uva.nl
-----------------------

The Distributed ASCI Supercomputer 4 (http://www.cs.vu.nl/das4/).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               all.q
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: das4.json <../../src/radical/pilot/configs/das4.json>`

archer.ac.uk
------------

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               standard
``sandbox``             /work/`id -gn`/`id -gn`/$USER
================== ============================

:download:`Raw Configuration file: epsrc.json <../../src/radical/pilot/configs/epsrc.json>`

sierra.futuregrid.org
---------------------

The FutureGrid 'sierra' cluster (https://futuregrid.github.io/manual/hardware.html).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

hotel.futuregrid.org
--------------------

The FutureGrid 'hotel' cluster (https://futuregrid.github.io/manual/hardware.html).

.. note::  Due to a broken MPI installation, 'hotel' is currently not usable as agents won't start up / run properly.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

india.futuregrid.org
--------------------

The FutureGrid 'india' cluster (https://futuregrid.github.io/manual/hardware.html).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

alamo.futuregrid.org
--------------------

The FutureGrid 'alamo' cluster (https://futuregrid.github.io/manual/hardware.html).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               short
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`

bigred2.uits.indiana.edu
------------------------

Indiana University's high-performance parallel computing cluster )https://kb.iu.edu/d/bcqt).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               None
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: iu.json <../../src/radical/pilot/configs/iu.json>`

quarry.uits.indiana.edu
-----------------------

The Quarry Linux cluster at Indiana University (https://kb.iu.edu/d/avkx).

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               None
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: iu.json <../../src/radical/pilot/configs/iu.json>`

localhost
---------

Your local machine.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               None
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: localhost.json <../../src/radical/pilot/configs/localhost.json>`

supermuc.lrz.de
---------------

The SuperMUC petascale HPC cluster at LRZ, Munich (http://www.lrz.de/services/compute/supermuc/systemdescription/).

.. note::  Authentication to SuperMUC is by means of X509, so make sure you can gsissh into the machine.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               test
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: lrz.json <../../src/radical/pilot/configs/lrz.json>`

yellowstone.ucar.edu
--------------------

The Yellowstone IBM iDataPlex cluster at UCAR (https://www2.cisl.ucar.edu/resources/yellowstone).

.. note::  We only support one concurrent CU per node currently.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               premium
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: ncar.json <../../src/radical/pilot/configs/ncar.json>`

tutorial.radical.org
--------------------

Our private tutorial VM on EC2

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: radical.json <../../src/radical/pilot/configs/radical.json>`

davinci.rice.edu
----------------

The DAVinCI Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+DAVinCI).

.. note::  DAVinCI compute nodes have 12 or 16 processor cores per node.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               parallel
``sandbox``             $SHARED_SCRATCH
================== ============================

:download:`Raw Configuration file: rice.json <../../src/radical/pilot/configs/rice.json>`

blacklight.psc.xsede.org
------------------------

The XSEDE 'Blacklight' cluster at PSC (https://www.psc.edu/index.php/computing-resources/blacklight).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               batch
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

stampede.tacc.utexas.edu
------------------------

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $WORK
================== ============================

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

lonestar.tacc.xsede.org
-----------------------

The XSEDE 'Lonestar' cluster at TACC (https://www.tacc.utexas.edu/user-services/user-guides/lonestar-user-guide).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

gordon.sdsc.xsede.org
---------------------

The XSEDE 'Gordon' cluster at SDSC (http://www.sdsc.edu/us/resources/gordon/).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

trestles.sdsc.xsede.org
-----------------------

The XSEDE 'Trestles' cluster at SDSC (http://www.sdsc.edu/us/resources/trestles/).

.. note::  Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.

Default values for ComputePilotDescription attributes:

================== ============================
Parameter               Value
================== ============================
``queue``               normal
``sandbox``             $HOME
================== ============================

:download:`Raw Configuration file: xsede.json <../../src/radical/pilot/configs/xsede.json>`

