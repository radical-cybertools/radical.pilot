

This is the SP1 implementation of Sinon's pilot API.  

Constants are imported directly from the API module.  

Classes all inherit from their respective API couterparts -- the classes there
will always raise NotImplemented, which will thus be the default for any
class/method whhich is not implemented in v1.

There are several cases where multiple inheritance is required, for example:


```
    def class ComputeUnit (sinon.v1.Unit, sinon.api.ComputeUnit) :
        ...
```

or

```
    def class Attributes (saga.Attributes, sinon.api.Attributes) :
        ...
```

In theses, the classes which provide the actual semantics need to be inherited
from first, and the api classes last -- otherwise we would get exceptions even
for implemented classes/methods.  We though *never* call the constructors of the
API classes, as those will always raise an exception.  



Data Model
==========

v1 uses Redis as central persistent data storage, accessed via the SAGA
Advert Package.  The data model is as follows: 

State for all entities is kept in a name space hierarchy, which separates
entities for individual users and application sessions:

    /sinon/v1/users/<username>/<sid>/

`UnitManager` and `PilotManager` are subdirs of that session root, under their
respective ID:

    /sinon/v1/users/<username>/<sid>/pm.1/
    /sinon/v1/users/<username>/<sid>/pm.2/
    /sinon/v1/users/<username>/<sid>/pm.3/
    /sinon/v1/users/<username>/<sid>/um.1/
    /sinon/v1/users/<username>/<sid>/um.2/

Submitting (unscheduled) units creates entries in the unit service directory:

    /sinon/v1/users/<username>/<sid>/pm.1/
    /sinon/v1/users/<username>/<sid>/pm.2/ 
    /sinon/v1/users/<username>/<sid>/pm.3/
    /sinon/v1/users/<username>/<sid>/um.1/u.1
    /sinon/v1/users/<username>/<sid>/um.2/

Pilot creation adds a pilot dir to the pilot manager subtree:

    /sinon/v1/users/<username>/<sid>/pm.1/
    /sinon/v1/users/<username>/<sid>/pm.2/
    /sinon/v1/users/<username>/<sid>/pm.2/p.1/
    /sinon/v1/users/<username>/<sid>/pm.3/
    /sinon/v1/users/<username>/<sid>/um.1/
    /sinon/v1/users/<username>/<sid>/um.1/u.1
    /sinon/v1/users/<username>/<sid>/um.2/

When a pilot is added to a unit manager, the unit manager dir gets a pilot
specific subdir, and the unit manager ID is added in the `UnitManagers`
attribute of the pilot dir:

    /sinon/v1/users/<username>/<sid>/pm.1/
    /sinon/v1/users/<username>/<sid>/pm.2/ 
    /sinon/v1/users/<username>/<sid>/pm.2/p.1/  {UnitManagers: [um.1]}
    /sinon/v1/users/<username>/<sid>/pm.3/
    /sinon/v1/users/<username>/<sid>/um.1/
    /sinon/v1/users/<username>/<sid>/um.1/u.1
    /sinon/v1/users/<username>/<sid>/um.1/p.1/
    /sinon/v1/users/<username>/<sid>/um.2/

Scheduling moves these units into the pilot directories:

    /sinon/v1/users/<username>/<sid>/pm.1/
    /sinon/v1/users/<username>/<sid>/pm.2/ 
    /sinon/v1/users/<username>/<sid>/pm.2/p.1/  {UnitManagers: [um.1]}
    /sinon/v1/users/<username>/<sid>/pm.3/
    /sinon/v1/users/<username>/<sid>/um.1/
    /sinon/v1/users/<username>/<sid>/um.1/p.1/u.1
    /sinon/v1/users/<username>/<sid>/um.2/

A pilot can be added to multiple unit services, and serve units from all of
those:

    /sinon/v1/users/<username>/<sid>/pm.1/
    /sinon/v1/users/<username>/<sid>/pm.2/ 
    /sinon/v1/users/<username>/<sid>/pm.2/p.1/  {UnitManagers: [um.1, um.2]}
    /sinon/v1/users/<username>/<sid>/pm.3/
    /sinon/v1/users/<username>/<sid>/um.1/
    /sinon/v1/users/<username>/<sid>/um.1/p.1/u.1
    /sinon/v1/users/<username>/<sid>/um.2/
    /sinon/v1/users/<username>/<sid>/um.2/p.1/u.2

Pilot agents can subscribe to events on a specific pilot, so they are aware when
pilots get added to new UMs, and when new units get added to their queue at
those UMs.  The UM instances in Sinon can subscribe to events from their pilots
and units, too, so that they get notified of state changes etc.

Scheduler lives in its own thread, is associated with a unit service, and gets
information about new unit submissions via a queue.  It is then tasked to move
the units around in the UM subtree, according to some clever scheme.

