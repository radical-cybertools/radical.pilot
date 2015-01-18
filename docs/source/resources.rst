

.. _chapter_resources:

List of Pre-Configured Resources
================================

FUTUREGRID
==========

SIERRA
******

The FutureGrid 'sierra' cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.sierra``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

ALAMO
*****

The FutureGrid 'alamo' cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.alamo``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : short``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

HOTEL
*****

The FutureGrid 'hotel' cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.hotel``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Note**            : Due to a broken MPI installation, 'hotel' is currently not usable as agents won't start up / run properly.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

INDIA
*****

The FutureGrid 'india' cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.india``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RADICAL
=======

TUTORIAL
********

Our private tutorial VM on EC2

* **Resource label**      : ``radical.tutorial``
* **Raw config**          : :download:`radical.json <../../src/radical/pilot/configs/radical.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local``

RICE
====

DAVINCI
*******

The DAVinCI Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+DAVinCI).

* **Resource label**      : ``rice.davinci``
* **Raw config**          : :download:`rice.json <../../src/radical/pilot/configs/rice.json>`
* **Note**            : DAVinCI compute nodes have 12 or 16 processor cores per node.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : parallel``
 * ``sandbox       : $SHARED_SCRATCH/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

BIOU
****

The Blue BioU Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+Blue+BioU).

* **Resource label**      : ``rice.biou``
* **Raw config**          : :download:`rice.json <../../src/radical/pilot/configs/rice.json>`
* **Note**            : Blue BioU compute nodes have 32 processor cores per node.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : serial``
 * ``sandbox       : $SHARED_SCRATCH/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

LOCAL
=====

LOCALHOST
*********

Your local machine.

* **Resource label**      : ``local.localhost``
* **Raw config**          : :download:`local.json <../../src/radical/pilot/configs/local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

NCAR
====

YELLOWSTONE
***********

The Yellowstone IBM iDataPlex cluster at UCAR (https://www2.cisl.ucar.edu/resources/yellowstone).

* **Resource label**      : ``ncar.yellowstone``
* **Raw config**          : :download:`ncar.json <../../src/radical/pilot/configs/ncar.json>`
* **Note**            : We only support one concurrent CU per node currently.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : premium``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

DAS4
====

FS2
***

The Distributed ASCI Supercomputer 4 (http://www.cs.vu.nl/das4/).

* **Resource label**      : ``das4.fs2``
* **Raw config**          : :download:`das4.json <../../src/radical/pilot/configs/das4.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : all.q``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

IU
==

BIGRED2
*******

Indiana University's HPC cluster (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2``
* **Raw config**          : :download:`iu.json <../../src/radical/pilot/configs/iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

QUARRY
******

The Quarry Linux cluster at Indiana University (https://kb.iu.edu/d/avkx).

* **Resource label**      : ``iu.quarry``
* **Raw config**          : :download:`iu.json <../../src/radical/pilot/configs/iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

EPSRC
=====

ARCHER
******

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

* **Resource label**      : ``epsrc.archer``
* **Raw config**          : :download:`epsrc.json <../../src/radical/pilot/configs/epsrc.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : standard``
 * ``sandbox       : /work/`id -gn`/`id -gn`/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

NERSC
=====

HOPPER
******

The Nersc Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper``
* **Raw config**          : :download:`nersc.json <../../src/radical/pilot/configs/nersc.json>`
* **Note**            : In a fresh virtualenv, run 'easy_install pip==1.2.1' to avoid ssl errors.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : debug``
 * ``sandbox       : /scratch/scratchdirs/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

LRZ
===

SUPERMUC
********

The SuperMUC petascale HPC cluster at LRZ, Munich (http://www.lrz.de/services/compute/supermuc/).

* **Resource label**      : ``lrz.supermuc``
* **Raw config**          : :download:`lrz.json <../../src/radical/pilot/configs/lrz.json>`
* **Note**            : Default authentication to SuperMUC uses X509 and is firewalled, make sure you can gsissh into the machine from your registered IP address. Because of outgoing traffic restrictions your MongoDB needs to run on a port in the range 20000 to 25000.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : test``
 * ``sandbox       : $HOME``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh``

XSEDE
=====

LONESTAR
********

The XSEDE 'Lonestar' cluster at TACC (https://www.tacc.utexas.edu/resources/hpc/lonestar).

* **Resource label**      : ``xsede.lonestar``
* **Raw config**          : :download:`xsede.json <../../src/radical/pilot/configs/xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

STAMPEDE
********

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**      : ``xsede.stampede``
* **Raw config**          : :download:`xsede.json <../../src/radical/pilot/configs/xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

GORDON
******

The XSEDE 'Gordon' cluster at SDSC (http://www.sdsc.edu/us/resources/gordon/).

* **Resource label**      : ``xsede.gordon``
* **Raw config**          : :download:`xsede.json <../../src/radical/pilot/configs/xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

TRESTLES
********

The XSEDE 'Trestles' cluster at SDSC (http://www.sdsc.edu/us/resources/trestles/).

* **Resource label**      : ``xsede.trestles``
* **Raw config**          : :download:`xsede.json <../../src/radical/pilot/configs/xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

BLACKLIGHT
**********

The XSEDE 'Blacklight' cluster at PSC (https://www.psc.edu/index.php/computing-resources/blacklight).

* **Resource label**      : ``xsede.blacklight``
* **Raw config**          : :download:`xsede.json <../../src/radical/pilot/configs/xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

