
.. _chapter_user_guide_07:

***********************
Sharing Unit Input Data
***********************

RP aims to support the concurrent execution of many tasks, and for many
workloads which fit that broad description, those tasks share (some or all)
input data.  We have seen :ref:`earlier <chapter_user_guide_05>` that input
staging can incur a significant runtime overhead -- but that can be
significantly reduced by avoiding redundant staging operations.

For this purpose, each RP `pilot` manages a space of shared data, and any data
put into that space by the application can later be symlinked into the unit's
workdir, for consumption:

.. code-block:: python

    # stage shared data from `pwd` to the pilot's shared staging space
    pilot.stage_in({'source': 'file://%s/input.dat' % os.getcwd(),
                    'target': 'staging:///input.dat',
                    'action': rp.TRANSFER})

    [...]

    for i in range(0, n):

        cud = rp.ComputeUnitDescription()

        cud.executable     = '/usr/bin/wc'
        cud.arguments      = ['-c', 'input.dat']
        cud.input_staging  = {'source': 'staging:///input.dat', 
                              'target': 'input.dat',
                              'action': rp.LINK}

The `rp.LINK` staging action requests a symlink to be created by RP, instead of
the copy operation used on the default `rp.TRANSFER` action.  The full example
can be found here: 
:download:`07_shared_unit_data.py <../../../examples/07_shared_unit_data.py>`.

.. note:: Unlike many other methods in RP, the `pilot.stage_in` option is
          *synchronous*, ie. it will only return once the transfer has been
          completed.  That semantics may change in a future version of RP.


Running the Example
-------------------

The result of this example's execution is the very same as we saw in the
:ref:`previous <chapter_user_guide_05>`, but it will now run significantly
faster due to the removed staging redundancy (at least for non-local pilots):

.. image:: 07_shared_unit_data.png


What's Next?
------------

This completes the discussion on data staging -- the next sections will go into
more details of the units execution: :ref:`environment setup <chapter_user_guide_08>`, 
:ref:`pre- and post- execution <chapter_user_guide_10>`, 
and :ref:`MPI applications <chapter_user_guide_09>`.

