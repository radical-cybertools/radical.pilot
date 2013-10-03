

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


