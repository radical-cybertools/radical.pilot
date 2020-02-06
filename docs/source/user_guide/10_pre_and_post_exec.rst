
.. _chapter_user_guide_10:

**********************************
Using Pre- and Post- exec commands
**********************************

In some cases, units need customizable setup routines. A frequent example is
the use of `module load` commands on various HPC platforms, which are used to
prepare the runtime environment of the unit's executable in a well defined,
system-specific way.

RP supports the invocation of such commands via the `pre_exec` and `post_exec`
keys for the unit description.  

.. note:: Pre- and Post- execution is performed on the *resource headnode*.
          Abusing these commands for any compute or I/O heavy load can lead
          to serious consequences, and will likely draw the wrath of the
          system administrators upon you! You have been warned...

The code example below shows the same environment setup we have been using in
an earlier section, but now rendered via a `pre_exec` command:

.. code-block:: python

    cud = rp.ComputeUnitDescription()

    cud.pre_exec    = ['export TEST=jabberwocky']
    cud.executable  = '/bin/echo'
    cud.arguments   = ['$RP_UNIT_ID greets $TEST']

which again will make the environment variable `TEST` available during the
execution of the unit.


Running the Example
-------------------

:download:`10_pre_and_post_exec.py <../../../examples/10_pre_and_post_exec.py>`.
uses the code above to run a bag of `echo` commands:

.. image:: 10_pre_and_post_exec.png


The RP User-Guide concludes with this section.  We recommend to check out the
RP API documentation next and then move on to write your own RP application.
You may find it useful to start from one of the examples of this guide, adding
your own code as needed. If you find any issue, do not hesitate to 
`open a ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_ 
and we will be happy to offer our support.

.. want to start off with the
.. :download:`canonical example <../../../examples/10_pre_and_post_exec.py>`, and
.. then add bits and pieces from the various user :ref:`<chapter_user_guide>`
.. sections as needed.
