

.. _chapter_resources:

List of Pre-Configured Resources
================================

RESOURCE_EPSRC
==============

ARCHER
******

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

* **Resource label**      : ``epsrc.archer``
* **Raw config**          : :download:`resource_epsrc.json <../../src/radical/pilot/configs/resource_epsrc.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : standard``
 * ``sandbox       : /work/`id -gn`/`id -gn`/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

ARCHER_ORTE
***********

The EPSRC Archer Cray XC30 system (https://www.archer.ac.uk/)

* **Resource label**      : ``epsrc.archer_orte``
* **Raw config**          : :download:`resource_epsrc.json <../../src/radical/pilot/configs/resource_epsrc.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : standard``
 * ``sandbox       : /work/`id -gn`/`id -gn`/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_RADICAL
================

TUTORIAL
********

Our private tutorial VM on EC2

* **Resource label**      : ``radical.tutorial``
* **Raw config**          : :download:`resource_radical.json <../../src/radical/pilot/configs/resource_radical.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local``

RESOURCE_STFC
=============

JOULE
*****

The STFC Joule IBM BG/Q system (http://community.hartree.stfc.ac.uk/wiki/site/admin/home.html)

* **Resource label**      : ``stfc.joule``
* **Raw config**          : :download:`resource_stfc.json <../../src/radical/pilot/configs/resource_stfc.json>`
* **Note**            : This currently needs a centrally administered outbound ssh tunnel.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : prod``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_NERSC
==============

EDISON_CCM
**********

The NERSC Edison Cray XC30 in Cluster Compatibility Mode (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison_ccm``
* **Raw config**          : :download:`resource_nersc.json <../../src/radical/pilot/configs/resource_nersc.json>`
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
* **Raw config**          : :download:`resource_nersc.json <../../src/radical/pilot/configs/resource_nersc.json>`
* **Note**            : 
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, go``

HOPPER
******

The NERSC Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper``
* **Raw config**          : :download:`resource_nersc.json <../../src/radical/pilot/configs/resource_nersc.json>`
* **Note**            : 
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, go``

HOPPER_APRUN
************

The NERSC Hopper Cray XE6 (https://www.nersc.gov/users/computational-systems/hopper/)

* **Resource label**      : ``nersc.hopper_aprun``
* **Raw config**          : :download:`resource_nersc.json <../../src/radical/pilot/configs/resource_nersc.json>`
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
* **Raw config**          : :download:`resource_nersc.json <../../src/radical/pilot/configs/resource_nersc.json>`
* **Note**            : For CCM you need to use special ccm_ queues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : ccm_queue``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

EDISON_APRUN
************

The NERSC Edison Cray XC30 (https://www.nersc.gov/users/computational-systems/edison/)

* **Resource label**      : ``nersc.edison_aprun``
* **Raw config**          : :download:`resource_nersc.json <../../src/radical/pilot/configs/resource_nersc.json>`
* **Note**            : Only one CU per node in APRUN mode
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : regular``
 * ``sandbox       : $SCRATCH``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, go``

RESOURCE_FUTUREGRID
===================

BRAVO
*****

FutureGrid Hewlett-Packard ProLiant compute cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.bravo``
* **Raw config**          : :download:`resource_futuregrid.json <../../src/radical/pilot/configs/resource_futuregrid.json>`
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
* **Raw config**          : :download:`resource_futuregrid.json <../../src/radical/pilot/configs/resource_futuregrid.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

ECHO
****

FutureGrid Supermicro ScaleMP cluster (https://futuregrid.github.io/manual/hardware.html).

* **Resource label**      : ``futuregrid.echo``
* **Raw config**          : :download:`resource_futuregrid.json <../../src/radical/pilot/configs/resource_futuregrid.json>`
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
* **Raw config**          : :download:`resource_futuregrid.json <../../src/radical/pilot/configs/resource_futuregrid.json>`
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
* **Raw config**          : :download:`resource_futuregrid.json <../../src/radical/pilot/configs/resource_futuregrid.json>`
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
* **Raw config**          : :download:`resource_futuregrid.json <../../src/radical/pilot/configs/resource_futuregrid.json>`
* **Note**            : Untested.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : delta``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_LOCAL
==============

LOCALHOST
*********

Your local machine.

* **Resource label**      : ``local.localhost``
* **Raw config**          : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**            : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : local``

* **Available schemas**   : ``local, ssh``

RESOURCE_NCSA
=============

BW_CCM
******

The NCSA Blue Waters Cray XE6/XK7 system in CCM (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_ccm``
* **Raw config**          : :download:`resource_ncsa.json <../../src/radical/pilot/configs/resource_ncsa.json>`
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
* **Raw config**          : :download:`resource_ncsa.json <../../src/radical/pilot/configs/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

BW_APRUN
********

The NCSA Blue Waters Cray XE6/XK7 system (https://bluewaters.ncsa.illinois.edu/)

* **Resource label**      : ``ncsa.bw_aprun``
* **Raw config**          : :download:`resource_ncsa.json <../../src/radical/pilot/configs/resource_ncsa.json>`
* **Note**            : Running 'touch .hushlogin' on the login node will reduce the likelihood of prompt detection issues.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : /scratch/sciteam/$USER``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh``

RESOURCE_XSEDE
==============

LONESTAR
********

The XSEDE 'Lonestar' cluster at TACC (https://www.tacc.utexas.edu/resources/hpc/lonestar).

* **Resource label**      : ``xsede.lonestar``
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
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
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $WORK``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh, go``

GORDON
******

The XSEDE 'Gordon' cluster at SDSC (http://www.sdsc.edu/us/resources/gordon/).

* **Resource label**      : ``xsede.gordon``
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : normal``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

COMET
*****

The Comet HPC resource at SDSC 'HPC for the 99%' (http://www.sdsc.edu/services/hpc/hpc_systems.html#comet).

* **Resource label**      : ``xsede.comet``
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : compute``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

SUPERMIC
********

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**      : ``xsede.supermic``
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**            : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : workq``
 * ``sandbox       : /work/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

TRESTLES
********

The XSEDE 'Trestles' cluster at SDSC (http://www.sdsc.edu/us/resources/trestles/).

* **Resource label**      : ``xsede.trestles``
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
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
* **Raw config**          : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**            : Always set the ``project`` attribute in the ComputePilotDescription or the pilot will fail.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, gsissh``

RESOURCE_RICE
=============

DAVINCI
*******

The DAVinCI Linux cluster at Rice University (https://docs.rice.edu/confluence/display/ITDIY/Getting+Started+on+DAVinCI).

* **Resource label**      : ``rice.davinci``
* **Raw config**          : :download:`resource_rice.json <../../src/radical/pilot/configs/resource_rice.json>`
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
* **Raw config**          : :download:`resource_rice.json <../../src/radical/pilot/configs/resource_rice.json>`
* **Note**            : Blue BioU compute nodes have 32 processor cores per node.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : serial``
 * ``sandbox       : $SHARED_SCRATCH/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_DAS4
=============

FS2
***

The Distributed ASCI Supercomputer 4 (http://www.cs.vu.nl/das4/).

* **Resource label**      : ``das4.fs2``
* **Raw config**          : :download:`resource_das4.json <../../src/radical/pilot/configs/resource_das4.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : all.q``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_NCAR
=============

YELLOWSTONE
***********

The Yellowstone IBM iDataPlex cluster at UCAR (https://www2.cisl.ucar.edu/resources/yellowstone).

* **Resource label**      : ``ncar.yellowstone``
* **Raw config**          : :download:`resource_ncar.json <../../src/radical/pilot/configs/resource_ncar.json>`
* **Note**            : We only support one concurrent CU per node currently.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : premium``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_LRZ
============

SUPERMUC
********

The SuperMUC petascale HPC cluster at LRZ, Munich (http://www.lrz.de/services/compute/supermuc/).

* **Resource label**      : ``lrz.supermuc``
* **Raw config**          : :download:`resource_lrz.json <../../src/radical/pilot/configs/resource_lrz.json>`
* **Note**            : Default authentication to SuperMUC uses X509 and is firewalled, make sure you can gsissh into the machine from your registered IP address. Because of outgoing traffic restrictions your MongoDB needs to run on a port in the range 20000 to 25000.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : test``
 * ``sandbox       : $HOME``
 * ``access_schema : gsissh``

* **Available schemas**   : ``gsissh, ssh``

RESOURCE_IU
===========

BIGRED2
*******

Indiana University's Cray XE6/XK7 cluster (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2``
* **Raw config**          : :download:`resource_iu.json <../../src/radical/pilot/configs/resource_iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : $HOME``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

BIGRED2_CCM
***********

Indiana University's Cray XE6/XK7 cluster in Cluster Compatibility Mode (CCM) (https://kb.iu.edu/d/bcqt).

* **Resource label**      : ``iu.bigred2_ccm``
* **Raw config**          : :download:`resource_iu.json <../../src/radical/pilot/configs/resource_iu.json>`
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : None``
 * ``sandbox       : /N/dc2/scratch/$USER``
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh``

RESOURCE_ORNL
=============

TITAN
*****

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/titan/)

* **Resource label**      : ``ornl.titan``
* **Raw config**          : :download:`resource_ornl.json <../../src/radical/pilot/configs/resource_ornl.json>`
* **Note**            : Requires the use of an RSA SecurID on every connection.
* **Default values** for ComputePilotDescription attributes:

 * ``queue         : batch``
 * ``sandbox       : $MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema : ssh``

* **Available schemas**   : ``ssh, local, go``

