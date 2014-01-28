
.. _chapter_machconf:

***************************
Resource Configration Files
***************************

Introduction
============

In order to keep SAGA-Pilot applications free from clutter and 
machine-specific parameters and constants, SAGA-Pilot uses 
resource configration files.

Machine configuration files are a mandatory parameter for 
creating a new :class:`sinon.PilotManager` instance::

    FGCONF = 'https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json'
    
    s = sinon.Session(database_url=DBURL)
    pm = sinon.PilotManager(session=s, resource_configurations=FGCONF)

A resource configuration has the following layout::

    {
        "futuregrid.INDIA": {
            "URL"  : "pbs+ssh://india.futuregrid.org",
            "key1" : "val",
            "key2" : "val"
        },

        "futuregrid.SIERRA": {
            "URL": "pbs+ssh://sierra.futuregrid.org",
            "key1" : "val",
            "key2" : "val"
        }
    }

In the example above, `futuregrid.INDIA` and `futuregrid.SIERRA` are the
**resource keys**. Resource keys are used in
:class:`sagapilot.ComputePilotDescription` to create a
:class:`sagapilot.ComputePilot` for a given machine::

    pd = sagapilot.ComputePilotDescription()
    pd.resource = "futuregrid.INDIA"  # Key defined in futuregrid.json
    pd.cores = 16

    pilot_india = pm.submit_pilots(pd)


Available Machine Files
=======================

We maintain a set of ready to use resource configuration files for some of the more 
popular cyberinfrastructure federations.

FutureGrid
----------

* Homepage: `http://www.futuregrid.org <http://www.futuregrid.org>`_
* Resource file URL: `https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json <https://raw.github.com/saga-project/saga-pilot/master/configs/futuregrid.json>`_

Write a New Machine File
========================
