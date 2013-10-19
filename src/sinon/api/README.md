

The main motivation to keep the API separated from the Sinon implementation is
its planned re-use for SP2.  Yes, this incurs a small code and call stack
overhead, but OTOH ensure API compatibility for Sinon consumers.

All Sinon documentation is created from this `sinon.api` module, all
implementation is done in the `sinon.v1` module.  The `sinon.__init__.py` will
load the `sinon.v1` implementation, so that is what an application uses -- but
all classes in `sinon.v1` will inherit from `sinon.api`, so that the API is
truthfully rendered.


