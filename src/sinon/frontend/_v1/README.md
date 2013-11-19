

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

FIXME

Pilot agents can subscribe to events on a specific pilot, so they are aware when
pilots get added to new UMs, and when new units get added to their queue at
those UMs.  The UM instances in Sinon can subscribe to events from their pilots
and units, too, so that they get notified of state changes etc.

Scheduler lives in its own thread, is associated with a unit service, and gets
information about new unit submissions via a queue.  It is then tasked to move
the units around in the UM subtree, according to some clever scheme.

