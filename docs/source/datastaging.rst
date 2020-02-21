.. _chapter_data_staging:

************
Data Staging
************

.. note:: Currently RADICAL-Pilot only supports data on file abstraction
          level, so `data == files` at this moment.

Many, if not all, programs require input data to operate and create output
data as a result in some form or shape. RADICAL-Pilot has a set of constructs
that allows the user to specify the required staging of input and output files
for a Compute Unit.

The primary constructs are on the level of the Compute Unit (Description)
which are discussed in the next section. For more elaborate use-cases we also
have constructs on the Compute Pilot level, which are discussed later in this
chapter.

.. note:: RADICAL-Pilot uses system calls for local file operations and SAGA for
          remote transfers and URL specification.


Compute Unit I/O
================

To instruct RADICAL-Pilot to handle files for you, there are two things to
take care of. First you need to specify the respective input and output files
for the Compute Unit in so called `staging directives`. Additionally you need
to associate these `staging directives` to the the Compute Unit by means of
the ``input_staging`` and ``output_staging`` members.

What it looks like
------------------

The following code snippet shows this in action:

.. code-block:: python

    INPUT_FILE_NAME  = "INPUT_FILE.TXT"
    OUTPUT_FILE_NAME = "OUTPUT_FILE.TXT"

    # This executes: "/usr/bin/sort -o OUTPUT_FILE.TXT INPUT_FILE.TXT"
    cud = radical.pilot.ComputeUnitDescription()
    cud.executable = "/usr/bin/sort"
    cud.arguments = ["-o", OUTPUT_FILE_NAME, INPUT_FILE_NAME]
    cud.input_staging  = INPUT_FILE_NAME
    cud.output_staging = OUTPUT_FILE_NAME

Here the `staging directives` ``INPUT_FILE_NAME`` and ``OUTPUT_FILE_NAME``
are simple strings that both specify a single filename and are associated to
the Compute Unit Description ``cud`` for input and output respectively.

What this does is that the file `INPUT_FILE.TXT` is transferred from the local
directory to the directory where the task is executed. After the task has run,
the file `OUTPUT_FILE.TXT` that has been created by the task, will be
transferred back to the local directory.

The :ref:`example-string` example demonstrates this in full glory.

Staging Directives
------------------

The format of the `staging directives` can either be a string as above or a
dict of the following structure:

.. code-block:: python

    staging_directive = {
        'source':   source,   # radical.pilot.Url() or string (MANDATORY).
        'target':   target,   # radical.pilot.Url() or string (OPTIONAL).
        'action':   action,   # One of COPY, LINK, MOVE, TRANSFER or TARBALL (OPTIONAL).
        'flags':    flags,    # Zero or more of CREATE_PARENTS or SKIP_FAILED (OPTIONAL).
        'priority': priority  # A number to instruct ordering (OPTIONAL).
    }

The semantics of the keys from the dict are as follows:

- ``source`` (default: None) and ``target`` (default:
  `os.path.basename(source)`): In case of the `staging directive` being used
  for *input*, then the ``source`` refers to the location to get the input
  files from,  e.g. the local working directory on your laptop or a remote
  data repository, and ``target`` refers to the working directory of the
  ComputeUnit. Alternatively, in case of the `staging directive` being used
  for *output*, then the ``source`` refers to the output files being generated
  by the ComputeUnit in the working directory and ``target`` refers to the
  location where you need to store the output data, e.g. back to your laptop
  or some remote data repository.

- ``action`` (default: TRANSFER): The *ultimate* goal is to make data
  available to the application kernel in the ComputeUnit and to be able to
  make the results available for further use. Depending on the relative
  location of the working directory of the ``source`` to the ``target``
  location, the action can be ``COPY`` (local resource), ``LINK`` (same file
  system), ``MOVE`` (local resource), ``TRANSFER`` (to a remote resource), or
  ``TARBALL`` (transfer to a remote resource after tarring files).

- ``flags`` (default: [CREATE_PARENTS, SKIP_FAILED]): By passing certain flags
  we can influence the behavior of the action. Available flags are:

    - ``CREATE_PARENTS``: Create parent directories while writing file.
    - ``SKIP_FAILED``: Don't stage out files if tasks failed.

    In case of multiple values these can be passed as a list.

- ``priority`` (default: 0): This optional field can be used to instruct the
  back end to priority the actions on the ``staging directives``. E.g. to
  first stage the output that is required for immediate further analysis and
  afterwards some output files that are of secondary concern.

The :ref:`example-dict` example demonstrates this in full glory.

When the `staging directives` are specified as a string as we did earlier,
that implies a `staging directive` where the ``source`` and the ``target``
are equal to the content of the string, the ``action`` is set to the default
action ``TRANSFER``, the ``flags`` are set to the default flags
``CREATE_PARENTS`` and ``SKIP_FAILED``, and the ``priority`` is set to the
default value ``0``:

.. code-block:: python

    'INPUT_FILE.TXT' == {
        'source':   'INPUT_FILE.TXT',
        'target':   'INPUT_FILE.TXT',
        'action':   TRANSFER,
        'flags':    [CREATE_PARENTS, SKIP_FAILED],
        'priority': 0
    }


.. _staging-area:

Staging Area
------------

As the pilot job creates an abstraction for a computational resource, the user
does not necessarily know where the working directory of the Compute Pilot or
the Compute Unit is. Even if he knows, the user might not have direct access
to it. For this situation we have the staging area, which is a special
construct so that the user can specify files relative to or in the working
directory without knowing the exact location. This can be done using the
following URL format:

.. code-block:: python

    'staging:///INPUT_FILE.TXT'

The :ref:`example-pipeline` example demonstrates this in full glory.


Compute Pilot I/O
=================

As mentioned earlier, in addition to the constructs on Compute Unit-level
RADICAL-Pilot also has constructs on Compute Pilot-level. The main rationale
for this is that often there is (input) data to be shared between multiple
Compute Units. Instead of transferring the same files for every Compute Unit,
we can transfer the data once to the Pilot, and then make it available to
every Compute Unit that needs it.

This works in a similar way as the Compute Unit-IO, where we use also use the
Staging Directive to specify the I/O transaction The difference is that in
this case, the Staging Directive is not associated to the Description, but
used in a direct method call ``pilot.stage_in(sd_pilot)``.

.. code-block:: python

    # Configure the staging directive for to insert the shared file into
    # the pilot staging directory.
    sd_pilot = {'source': shared_input_file_url,
                'target': os.path.join(MY_STAGING_AREA, SHARED_INPUT_FILE),
                'action': radical.pilot.TRANSFER
    }
    # Synchronously stage the data to the pilot
    pilot.stage_in(sd_pilot)

The :ref:`example-shared` example demonstrates this in full glory.

.. note:: The call to ``stage_in()`` is synchronous and will return once the
          transfer is complete.


Examples
========

.. note:: All of the following examples are configured to run on localhost,
          but they can be easily changed to run on a remote resource by
          modifying the resource specification in the Compute Pilot Description.
          Also note the comments in :ref:`staging-area` when changing the
          examples to a remote target.

These examples require an installation of RADICAL-Pilot of course. There are
download links for each of the examples.


.. _example-string:

String-Based Input and Output Transfer
--------------------------------------

This example demonstrates the simplest form of the data staging capabilities.
The example demonstrates how a local input file is staged through
RADICAL-Pilot, processed by the Compute Unit and the resulting output file is
staged back to the local environment.

.. note:: Download the example:
          ``curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/data_staging/io_staging_simple.py``

.. literalinclude:: ../../examples/data_staging/io_staging_simple.py


.. _example-dict:

Dictionary-Based Input and Output Transfer
------------------------------------------

This example demonstrates the use of the staging directives structure to have
more control over the staging behavior. The flow of the example is similar to
that of the previous example, but here we show that by using the dict-based
Staging Directive, one can specify different names and paths for the local and
remote files, a feature that is often required in real-world applications.

.. note:: Download the example:
          ``curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/data_staging/io_staging_dict.py``

.. literalinclude:: ../../examples/data_staging/io_staging_dict.py


.. _example-shared:

Shared Input Files
------------------

This example demonstrates the staging of a shared input file by means of the
stage_in() method of the pilot and consequently making that available to all
compute units.

.. note:: Download the example:
          ``curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/data_staging/io_staging_shared.py``

.. literalinclude:: ../../examples/data_staging/io_staging_shared.py


.. _example-pipeline:

Pipeline
--------

This example demonstrates a two-step pipeline that makes use of a remote pilot
staging area, where the first step of the pipeline copies the intermediate
output into and that is picked up by the second step in the pipeline.

.. note:: Download the example:
          ``curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/data_staging/io_staging_pipeline.py``

.. literalinclude:: ../../examples/data_staging/io_staging_pipeline.py
