.. _chapter_env_variables:

=====================
Environment Variables
=====================

Several aspects of RADICAL-Pilot (RP) behavior can/must be configured via
environment variables. Those variables are exported in the shell from which you
will launch your RP application. Usually, environment variables can be set using
the `export <https://manpages.org/export>`_ command.

.. warning:: Tables are large, scroll to the right to see the whole table.

End user
--------

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Name
      - Description
      - Default value
    * - .. envvar:: RADICAL_BASE
      - Root directory where to save temporary state files
      - `$HOME/.radical/`
    * - .. envvar:: RADICAL_UTILS_NTPHOST
      - NTP host used for profile syncing
      - `0.pool.ntp.org`
    * - .. envvar:: RADICAL_PILOT_BULK_CB
      - Enables bulk callbacks to boost performance. This changes the callback signature
      - FALSE
    * - .. envvar:: RADICAL_PILOT_STRICT_CANCEL
      - Limits task cancelation by not forcing the state "CANCELLED" on the Task Manager.
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEFAULT_LOG_TGT
      - Log target
      - .
    * - .. envvar:: RADICAL_DEFAULT_LOG_DIR
      - Log directory
      - $PWD
    * - .. envvar:: RADICAL_DEFAULT_LOG_LVL
      - The default log level when not explicitly set
      - ERROR
    * - .. envvar:: RADICAL_DEFAULT_REPORT
      - Flag to turn reporting on [TRUE/1] or off [FALSE/0/OFF]
      - TRUE
    * - .. envvar:: RADICAL_DEFAULT_REPORT_TGT
      - List of comma separated targets [0/null, 1/stdout, 2/stderr, ./{report_name/path}]
      - stderr
    * - .. envvar:: RADICAL_DEFAULT_REPORT_DIR
      - Directory used by the reporter module
      - $PWD
    * - .. envvar:: RADICAL_DEFAULT_PROFILE
      - Flag to turn profiling/tracing on [TRUE/1] or off [FALSE/0/OFF]
      - TRUE
    * - .. envvar:: RADICAL_DEFAULT_PROFILE_DIR
      - Directory where to store profiles/traces
      - $PWD

Logger
------

`ru.Logger` instances have a name and a name space.

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Name
      - Description
      - Default value
    * - .. envvar:: <NS>_LOG_LVL
      - Logging level ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
      - Refer `RADICAL_DEFAULT_*`
    * - .. envvar:: <NS>_LOG_TGT
      - Used for the log targets
      - Refer `RADICAL_DEFAULT_*`

.. note:: The name space is used to derive environmental variable names for log levels and targets. If no name space is given, it is derived from the variable name. For example, the name ``radical.pilot`` becomes ``RADICAL_PILOT``.

.. note:: ``<NS>_LOG_LVL`` controls the debug output for a corresponding namespace (NS), where NS can be applied as for a specific package (e.g., ``RADICAL_PILOT_LOG_LVL`` or ``RADICAL_UTILS_LOG_LVL``) or for a whole stack (e.g., ``RADICAL_LOG_LVL``).

Reporter
--------

`ru.Reporter` instances are very similar to `ru.Logger` instances: same schema is used for names and name spaces.

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Name
      - Description
      - Default value
    * - .. envvar:: <NS>_REPORT
      - Boolean to turn on and off Reporter
      - TRUE
    * - .. envvar:: <NS>_LOG_TGT
      - Where to report to.
      - {NOT_SET}

.. note:: ``<NS>_LOG_TGT`` is a list of comma separated targets ["0"/"null", "1"/"stdout", "2"/"stderr", "."/"<log_name>"] where to write the debug output for a corresponding namespace (NS).

Developers
----------

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Name
      - Description
      - Default value
    * - .. envvar:: RADICAL_UTILS_NO_ATFORK
      - Disables monkeypatching
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEBUG
      - Enables scattered debug facilities. This will likely slow down and even destabilize the code
      - {NOT_SET}
    * - .. envvar:: RU_RAISE_ON_\*
      - Related to :envvar:`RADICAL_DEBUG`, triggers specific exceptions
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEBUG_HELPER
      - Related to :envvar:`RADICAL_DEBUG`, enables a persistent debug helper class in the code and installs some signal handlers for extra debug output
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEBUG_VERBOSE
      - Related to :envvar:`RADICAL_DEBUG`, enables verbose messages for debugging. Controls "debug" module to collect stack traces. Verbose flag sets the level of details for output messages
      - {NOT_SET}
    * - .. envvar:: \*_PROFILE
      - `Profiler` is similar to `Logger` and `Reporter`
      - {NOT_SET}
    * - .. envvar:: RADICAL_PILOT_PRUN_VERBOSE
      - Increase verbosity of `prun` output
      - FALSE
    * - .. envvar:: UMS_OMPIX_PRRTE_DIR
      - Installation directory for PMIx/PRRTE used in RP LM PRTE (optional, to be obsolete)
      - {NOT_SET}
    * - .. envvar:: RADICAL_SAGA_SMT
      - Sets SMT settings on some resources. Usually configured via resource config options
      - 1
    * - .. envvar:: RP_PROF_DEBUG
      - Enables additional debug messages on profile extraction
      - {NOT_SET}

SAGA
----

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Name
      - Description
      - Default value
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_PROMPT
      - Prompt Pattern. Use this regex to detect shell prompts
      - [\\$#%>\\]]\\s*$
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_COPYMODE
      - Use the specified protocol for pty-level file transfer
      - options: 'sftp', 'scp', 'rsync+ssh', 'rsync'
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_SHAREMODE
      - Use the specified mode as flag for the ssh ControlMaster
      - options: 'auto', 'no' (This should be set to "no" on CentOS)
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_TIMEOUT
      - Connection timeout (in seconds) for the SAGA PTY layer. Connection timeout should be set to 60 or more
      - `10.0`
    * - .. envvar:: RADICAL_SAGA_PTY_CONN_POOL_SIZE
      - Maximum number of connections kept in a connection pool
      - 10
    * - .. envvar:: RADICAL_SAGA_PTY_CONN_POOL_TTL
      - Minimum time a connection is kept alive in a connection pool
      - 600
    * - .. envvar:: RADICAL_SAGA_PTY_CONN_POOL_WAIT
      - Maximum number of seconds to wait for any connection in the connection pool to become available before raising a timeout error
      - 600

Deprecated
----------

.. list-table::
    :widths: auto
    :header-rows: 1

    * - Name
      - Description
    * - .. envvar:: RP_ENABLE_OLD_DEFINES
      - Enables backward compatibility for old state defines
