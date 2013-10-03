.. _security_contexts:

Descriptions
************

Description Base Class -- :mod:`sinon.description`
--------------------------------------------------

.. automodule:: sinon.description
   :show-inheritance:
   :members: Description


**Example**::

    ctx = saga.Context("SSH")

    ctx.user_id   = "johndoe"
    ctx.user_key  = "/home/johndoe/.ssh/key_for_machine_x"
    ctx.user_pass = "XXXX"  # password to decrypt 'user_key' (if required)

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("ssh://machine_x.futuregrid.org",
                          session=session)


