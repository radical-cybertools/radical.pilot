.. _chapter_resources:

List of Pre-Configured Resources
================================

RESOURCE_PRINCETON
==================

TIGER_CPU
*********

* **Resource label**    : ``princeton.tiger_cpu``
* **Raw config**        : :download:`resource_princeton.json <../../src/radical/pilot/configs/resource_princeton.json>`
* **Note**              : 
* **Default values** for PilotDescription attributes:

 * ``queue         :cpu``
 * ``sandbox       :/scratch/gpfs/$USER/``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

TIGER_GPU
*********

* **Resource label**    : ``princeton.tiger_gpu``
* **Raw config**        : :download:`resource_princeton.json <../../src/radical/pilot/configs/resource_princeton.json>`
* **Note**              : 
* **Default values** for PilotDescription attributes:

 * ``queue         :gpu``
 * ``sandbox       :/scratch/gpfs/$USER/``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

RESOURCE_FUB
============

ALLEGRO_RSH
***********

The FU Berlin 'Allegro' cluster at IMP (http://www.allegro.imp.fu-berlin.de).

* **Resource label**    : ``fub.allegro_rsh``
* **Raw config**        : :download:`resource_fub.json <../../src/radical/pilot/configs/resource_fub.json>`
* **Note**              : This one uses experimental RSH support to execute tasks.
* **Default values** for PilotDescription attributes:

 * ``queue         :micro``
 * ``sandbox       :$HOME/NO_BACKUP``
 * ``access_schema :ssh``

* **Available schemas** : ``ssh``

RESOURCE_ORNL
=============

RHEA_APRUN
**********

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/titan/)

* **Resource label**    : ``ornl.rhea_aprun``
* **Raw config**        : :download:`resource_ornl.json <../../src/radical/pilot/configs/resource_ornl.json>`
* **Note**              : Requires the use of an RSA SecurID on every connection.
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema :local``

* **Available schemas** : ``local, ssh, go``

RHEA_SSH
********

The Cray XK7 supercomputer located at the Oak Ridge Leadership Computing Facility (OLCF), (https://www.olcf.ornl.gov/rhea/)

* **Resource label**    : ``ornl.rhea_ssh``
* **Raw config**        : :download:`resource_ornl.json <../../src/radical/pilot/configs/resource_ornl.json>`
* **Note**              : Requires the use of an RSA SecurID on every connection.
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$MEMBERWORK/`groups | cut -d' ' -f2```
 * ``access_schema :local``

* **Available schemas** : ``local, ssh, go``

SUMMIT
******

ORNL's summit, a Cray XK7

* **Resource label**    : ``ornl.summit``
* **Raw config**        : :download:`resource_ornl.json <../../src/radical/pilot/configs/resource_ornl.json>`
* **Note**              : None
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$MEMBERWORK/%(pd.project)s``
 * ``access_schema :local``

* **Available schemas** : ``local``

SUMMIT_PRTE
***********

ORNL's summit, a Cray XK7

* **Resource label**    : ``ornl.summit_prte``
* **Raw config**        : :download:`resource_ornl.json <../../src/radical/pilot/configs/resource_ornl.json>`
* **Note**              : None
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$MEMBERWORK/%(pd.project)s``
 * ``access_schema :local``

* **Available schemas** : ``local``

RESOURCE_NCAR
=============

CHEYENNE
********

An SGI ICE XA Cluster located at the National Center for Atmospheric Research (NCAR), (https://www2.cisl.ucar.edu/resources/computational-systems/cheyenne)

* **Resource label**    : ``ncar.cheyenne``
* **Raw config**        : :download:`resource_ncar.json <../../src/radical/pilot/configs/resource_ncar.json>`
* **Note**              : Requires the use of a token from an USB on every connection.
* **Default values** for PilotDescription attributes:

 * ``queue         :regular``
 * ``sandbox       :$TMPDIR``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

RESOURCE_LOCAL
==============

LOCALHOST
*********

Your local machine.

* **Resource label**    : ``local.localhost``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_APRUN
***************

Your local machine.

* **Resource label**    : ``local.localhost_aprun``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_YARN
**************

Your local machine.

* **Resource label**    : ``local.localhost_yarn``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_ANACONDA
******************

Your local machine.

* **Resource label**    : ``local.localhost_anaconda``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_SPARK
***************

Your local machine gets spark.

* **Resource label**    : ``local.localhost_spark``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_SPARK_ANACONDA
************************

Your local machine gets spark.

* **Resource label**    : ``local.localhost_spark_anaconda``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_ORTE
**************

Your local machine.

* **Resource label**    : ``local.localhost_orte``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_PRTE
**************

Your local machine.

* **Resource label**    : ``local.localhost_prte``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_ORTELIB
*****************

Your local machine.

* **Resource label**    : ``local.localhost_ortelib``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : To use the ssh schema, make sure that ssh access to localhost is enabled.
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

LOCALHOST_FUNCS
***************



* **Resource label**    : ``local.localhost_funcs``
* **Raw config**        : :download:`resource_local.json <../../src/radical/pilot/configs/resource_local.json>`
* **Note**              : 
* **Default values** for PilotDescription attributes:

 * ``queue         :None``
 * ``sandbox       :$HOME``
 * ``access_schema :local``

* **Available schemas** : ``local, ssh``

RESOURCE_RADICAL
================

TUTORIAL
********

Our private tutorial VM on EC2

* **Resource label**    : ``radical.tutorial``
* **Raw config**        : :download:`resource_radical.json <../../src/radical/pilot/configs/resource_radical.json>`
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$HOME``
 * ``access_schema :ssh``

* **Available schemas** : ``ssh, local``

ONE
***

radical server 1

* **Resource label**    : ``radical.one``
* **Raw config**        : :download:`resource_radical.json <../../src/radical/pilot/configs/resource_radical.json>`
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$HOME``
 * ``access_schema :ssh``

* **Available schemas** : ``ssh, local``

TWO
***

radical server 2

* **Resource label**    : ``radical.two``
* **Raw config**        : :download:`resource_radical.json <../../src/radical/pilot/configs/resource_radical.json>`
* **Default values** for PilotDescription attributes:

 * ``queue         :batch``
 * ``sandbox       :$HOME``
 * ``access_schema :ssh``

* **Available schemas** : ``ssh, local``

RESOURCE_XSEDE
==============

WRANGLER_SSH
************

The XSEDE 'Wrangler' cluster at TACC (https://www.tacc.utexas.edu/wrangler/).

* **Resource label**    : ``xsede.wrangler_ssh``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Always set the ``project`` attribute in the PilotDescription or the pilot will fail.
* **Default values** for PilotDescription attributes:

 * ``queue         :normal``
 * ``sandbox       :$WORK``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh, go``

WRANGLER_YARN
*************

The XSEDE 'Wrangler' cluster at TACC (https://www.tacc.utexas.edu/wrangler/).

* **Resource label**    : ``xsede.wrangler_yarn``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Always set the ``project`` attribute in the PilotDescription or the pilot will fail.
* **Default values** for PilotDescription attributes:

 * ``queue         :hadoop``
 * ``sandbox       :$WORK``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh, go``

WRANGLER_SPARK
**************

The XSEDE 'Wrangler' cluster at TACC (https://www.tacc.utexas.edu/wrangler/).

* **Resource label**    : ``xsede.wrangler_spark``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Always set the ``project`` attribute in the PilotDescription or the pilot will fail.
* **Default values** for PilotDescription attributes:

 * ``queue         :normal``
 * ``sandbox       :$WORK``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh, go``

FRONTERA
********

* **Resource label**    : ``xsede.frontera``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : 
* **Default values** for PilotDescription attributes:

 * ``queue         :normal``
 * ``sandbox       :$SCRATCH``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh, local``

STAMPEDE2_SSH
*************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**    : ``xsede.stampede2_ssh``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Always set the ``project`` attribute in the PilotDescription or the pilot will fail.
* **Default values** for PilotDescription attributes:

 * ``queue         :normal``
 * ``sandbox       :$WORK``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh``

STAMPEDE2_SRUN
**************

The XSEDE 'Stampede' cluster at TACC (https://www.tacc.utexas.edu/stampede/).

* **Resource label**    : ``xsede.stampede2_srun``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Always set the ``project`` attribute in the PilotDescription or the pilot will fail.
* **Default values** for PilotDescription attributes:

 * ``queue         :normal``
 * ``sandbox       :$WORK``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh``

SUPERMIC_SSH
************

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**    : ``xsede.supermic_ssh``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for PilotDescription attributes:

 * ``queue         :workq``
 * ``sandbox       :/work/$USER``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh``

SUPERMIC_ORTE
*************

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**    : ``xsede.supermic_orte``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for PilotDescription attributes:

 * ``queue         :workq``
 * ``sandbox       :/work/$USER``
 * ``access_schema :local``

* **Available schemas** : ``local, gsissh, ssh``

SUPERMIC_ORTELIB
****************

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**    : ``xsede.supermic_ortelib``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for PilotDescription attributes:

 * ``queue         :workq``
 * ``sandbox       :/work/$USER``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh``

BRIDGES
*******

The XSEDE 'Bridges' cluster at PSC (https://portal.xsede.org/psc-bridges/).

* **Resource label**    : ``xsede.bridges``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Always set the ``project`` attribute in the PilotDescription.
* **Default values** for PilotDescription attributes:

 * ``queue         :RM``
 * ``sandbox       :$SCRATCH``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh, go``

SUPERMIC_SPARK
**************

SuperMIC (pronounced 'Super Mick') is Louisiana State University's (LSU) newest supercomputer funded by the National Science Foundation's (NSF) Major Research Instrumentation (MRI) award to the Center for Computation & Technology. (https://portal.xsede.org/lsu-supermic)

* **Resource label**    : ``xsede.supermic_spark``
* **Raw config**        : :download:`resource_xsede.json <../../src/radical/pilot/configs/resource_xsede.json>`
* **Note**              : Partially allocated through XSEDE. Primary access through GSISSH. Allows SSH key authentication too.
* **Default values** for PilotDescription attributes:

 * ``queue         :workq``
 * ``sandbox       :/work/$USER``
 * ``access_schema :gsissh``

* **Available schemas** : ``gsissh, ssh``

RESOURCE_DEBUG
==============

LOCAL
*****



* **Resource label**    : ``debug.local``
* **Raw config**        : :download:`resource_debug.json <../../src/radical/pilot/configs/resource_debug.json>`
* **Note**              : 
* **Default values** for PilotDescription attributes:

 * ``queue         :``
 * ``sandbox       :$HOME/``
 * ``access_schema :local``

* **Available schemas** : ``local``

SUMMIT
******



* **Resource label**    : ``debug.summit``
* **Raw config**        : :download:`resource_debug.json <../../src/radical/pilot/configs/resource_debug.json>`
* **Note**              : 
* **Default values** for PilotDescription attributes:

 * ``queue         :``
 * ``sandbox       :$HOME/``
 * ``access_schema :local``

* **Available schemas** : ``local``

