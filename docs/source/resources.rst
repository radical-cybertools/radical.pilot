

.. _chapter_resources:

List of Pre-Configured Resources
================================

FUTUREGRID
==========

BRAVO
*****

FutureGrid Hewlett-Packard ProLiant compute cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.bravo``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Note**            : Works only up to 64 cores, beyond that Torque configuration is broken.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : bravo``
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

ECHO
****

FutureGrid Supermicro ScaleMP cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.echo``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Note**            : Untested
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : echo``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

XRAY
****

FutureGrid Cray XT5m cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.xray``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Note**            : One needs to add 'module load torque' to ~/.profile on xray.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : /scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

XRAY_CCM
********

FutureGrid Cray XT5m cluster in Cluster Compatibility Mode (CCM) (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.xray_ccm``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Note**            : One needs to add 'module load torque' to ~/.profile on xray.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : /scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

DELTA
*****

FutureGrid Supermicro GPU cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.delta``
* **Raw config**          : :download:`futuregrid.json <../../src/radical/pilot/configs/futuregrid.json>`
* **Note**            : Untested.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : delta``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

STFC
====

JOULE
*****

The STFC Joule IBM BG/Q system (http://community.hartree.stfc.ac.uk/wiki/site/admin/home.html)

* **Resource label**      : ``stfc.joule``
* **Raw config**          : :download:`stfc.json <../../src/radical/pilot/configs/stfc.json>`
* **Note**            : This currently needs a centrally administered outbound ssh tunnel.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : prod``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

NCSA
====

BW_CCM
******

The NCSA Blue Waters Cray XE6/XK7 system in CCM (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_ccm``
* **Raw config**          : :download:`ncsa.json <../../src/radical/pilot/configs/ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

BW
**

The NCSA Blue Waters Cray XE6/XK7 system (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw``
* **Raw config**          : :download:`ncsa.json <../../src/radical/pilot/configs/ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

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

LSU
===

SUPERMIC
********

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**      : ``lsu.supermic``
* **Raw config**          : :download:`lsu.json <../../src/radical/pilot/configs/lsu.json>`
* **Note**            : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : workq``
 * ``sandbox       : /work/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

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

Indiana University's Cray XE6/XK7 cluster (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2``
* **Raw config**          : :download:`iu.json <../../src/radical/pilot/configs/iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

BIGRED2_CCM
***********

Indiana University's Cray XE6/XK7 cluster in Cluster Compatibility Mode (CCM) (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2_ccm``
* **Raw config**          : :download:`iu.json <../../src/radical/pilot/configs/iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : /N/dc2/scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

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

EDISON_CCM
**********

The NERSC Edison Cray XC30 in Cluster Compatibility Mode (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison_ccm``
* **Raw config**          : :download:`nersc.json <../../src/radical/pilot/configs/nersc.json>`
* **Note**            : For CCM you need to use special ccm_ queues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

EDISON
******

The NERSC Edison Cray XC30 (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison``
* **Raw config**          : :download:`nersc.json <../../src/radical/pilot/configs/nersc.json>`
* **Note**            : Only one CU per node in APRUN mode
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

HOPPER
******

The NERSC Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper``
* **Raw config**          : :download:`nersc.json <../../src/radical/pilot/configs/nersc.json>`
* **Note**            : Only one CU per node in APRUN mode
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

HOPPER_CCM
**********

The NERSC Hopper Cray XE6 in Cluster Compatibility Mode (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper_ccm``
* **Raw config**          : :download:`nersc.json <../../src/radical/pilot/configs/nersc.json>`
* **Note**            : For CCM you need to use special ccm_ queues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : $SCRATCH``
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

ORNL
====

TITAN
*****

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/titan/)

* **Resource label**      : ``ornl.titan``
* **Raw config**          : :download:`ornl.json <../../src/radical/pilot/configs/ornl.json>`
* **Note**            : Requires the use of an RSA SecurID on every connection.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local``

