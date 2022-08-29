List of Env vars
__________________


.. list-table:: User List
    :widths: 35 100 20
    :header-rows: 1

    * - Environment Variable Name
      - Description
      - Default value
    * - .. envvar:: RADICAL_BASE
      - root dir for temporary state files,
      - `$HOME/.radical/`
    * - .. envvar:: RADICAL_PILOT_DBURL
      - MongoDB URL string and mandatory for RP to work.
      - {NOT_SET}
    * - .. envvar:: RADICAL_UTILS_NTPHOST
      - NTP host to be used for profile syncing
      - `0.pool.ntp.org`
    * - .. envvar:: RADICAL_PILOT_BULK_CB
      - Enable bulk callbacks for performance boost. This changes the callback signature.
      - FALSE
    * - .. envvar:: RADICAL_PILOT_STRICT_CANCEL
      - Limit task cancelation, not to force state "CANCELLED" on a Task Manager side. Note that corresponding command for pilot(s) to cancel tasks is published anyway
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEFAULT_LOG_TGT
      - The log target.
      - .
    * - .. envvar:: RADICAL_DEFAULT_LOG_DIR
      - The Log dir
      - $PWD
    * - .. envvar:: RADICAL_DEFAULT_LOG_LVL
      - The default log level when not set
      - ERROR
    * - .. envvar:: RADICAL_DEFAULT_REPORT
      - Flag to turn reporting on [TRUE/1] or off [FALSE/0/OFF]
      - TRUE
    * - .. envvar:: RADICAL_DEFAULT_REPORT_TGT
      - Similar to ..LOG_TGT: List of comma separated targets [0/null, 1/stdout, 2/stderr, ./{report_name/path}] to write the report info.
      - stderr
    * - .. envvar:: RADICAL_DEFAULT_REPORT_DIR
      - Directory of Reporter module
      - $PWD
    * - .. envvar:: RADICAL_DEFAULT_PROFILE
      - Flag to turn profiling/tracing on [TRUE/1] or off [FALSE/0/OFF]
      - TRUE
    * - .. envvar:: RADICAL_DEFAULT_PROFILE_DIR
      - Directory for profiles/traces
      - $PWD

.. raw:: html

   <hr>

.. list-table:: Logging-related Env vars.
    `ru.Logger` instances have a name and a name space.
    :widths: 35 100 20
    :header-rows: 1

    * - Environment Variable Name
      - Description
      - Default value
    * - <NS>_LOG_LVL
      - Logging level ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] to control the debug output for a corresponding namespace NS, where NS can be applied as for a specific package (e.g., "RADICAL_PILOT_LOG_LVL" or "RADICAL_UTILS_LOG_LVL") or for a whole stack (e.g., "RADICAL_LOG_LVL").
      - Refer `RADICAL_DEFAULT_*`
    * - <NS>_LOG_TGT
      - Used for the log targets
      - Refer `RADICAL_DEFAULT_*`

.. note:: The name space is used to derive env variable names for log levels and targets. If no ns is given, the ns is derived from the name. Eg. the name `radical.pilot` becomes `RADICAL_PILOT`.

.. raw:: html

   <hr>

.. list-table:: Reporter-related Env vars. `ru.Reporter` instances are very similar to `ru.Logger` instances: same schema is used for names and name spaces.
    :widths: 35 100 20
    :header-rows: 1

    * - Environment Variable Name
      - Description
      - Default value
    * - <NS>_REPORT
      - Boolean to turn on and off Reporter
      - TRUE
    * - <NS>_LOG_TGT
      - Where to report to. List of comma separated targets ["0"/"null", "1"/"stdout", "2"/"stderr", "."/"<log_name>"] to write the debug output for a corresponding namespace NS.
      - {NOT_SET}

.. raw:: html

   <hr>

.. list-table:: Developer List
    :widths: 35 100 20
    :header-rows: 1

    * - Environment Variable Name
      - Description
      - Default value
    * - .. envvar:: RADICAL_UTILS_NO_ATFORK
      - Monkeypatching can be disabled by setting RADICAL_UTILS_NO_ATFORK.
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEBUG
      - enables scattered debug facilities. This will likely slow down and even destabilize the code.
      - {NOT_SET}
    * - RU_RAISE_ON_*
      - related to :envvar:`RADICAL_DEBUG` to trigger specific exceptions
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEBUG_HELPER
      - related to :envvar:`RADICAL_DEBUG`, enables a persistent debug helper class in the code and installs some signal handlers for extra debug output.
      - {NOT_SET}
    * - .. envvar:: RADICAL_DEBUG_VERBOSE
      - related to :envvar:`RADICAL_DEBUG`, enables verbose messages for debugging. Controls "debug" module to collect stack traces. Verbose flag sets the level of details for output messages.
      - {NOT_SET}
    * - *_PROFILE
      - `Profiler` is similar to `Logger` and `Reporter`
      - {NOT_SET}
    * - .. envvar:: RADICAL_PILOT_PRUN_VERBOSE
      - Increase verbosity of prun output
      - FALSE
    * - .. envvar:: UMS_OMPIX_PRRTE_DIR
      - Installation directory for PMIx/PRRTE used in RP LM PRTE (optional, to be obsolete).
      - {NOT_SET}
    * - .. envvar:: RADICAL_SAGA_SMT
      - Sets SMT settings on some resources. Usually covered via resource config options
      - 1
    * - .. envvar:: RP_PROF_DEBUG
      - enable additional debug messagRP_PROF_DEBUGes on profile extraction
      - {NOT_SET}

.. raw:: html

   <hr>

.. list-table:: SAGA related vars
    :widths: 35 100 20
    :header-rows: 1

    * - Environment Variable Name
      - Description
      - Default Value
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_PROMPT
      - Prompt Pattern. Use this regex to detect shell prompts
      - [\\$#%>\\]]\\s*$
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_COPYMODE
      - Use the specified protocol for pty level file transfer
      - options: 'sftp', 'scp', 'rsync+ssh', 'rsync'
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_SHAREMODE
      - Use the specified mode as flag for the ssh ControlMaster
      - options: 'auto', 'no' (This should be set to "no" on CentOS)
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_TIMEOUT
      - Sets the connection timeout (in seconds, default: 10) for the SAGA PTY layer. On connection timeouts should be set to 60 or more
      - `10.0`
    * - .. envvar:: RADICAL_SAGA_PTY_CONN_POOL_SIZE
      - Maximum number of connections kept in a connection pool
      - 10
    * - .. envvar:: RADICAL_SAGA_PTY_CONN_POOL_TTL
      - Minimum time a connection is kept alive in a connection pool
      - 600
    * - .. envvar:: RADICAL_SAGA_PTY_CONN_POOL_WAIT
      - maximum number of seconds to wait for any connection in the connection pool to become available before raising a timeout error
      - 600

.. raw:: html

   <hr>

.. list-table:: Deprecated / Being Phased Out / No Longer valid
    :widths: 35 100
    :header-rows: 1

    * - Environment Variable Name
      - Description
    * - .. envvar:: RP_ENABLE_OLD_DEFINES
      - enable backward compatibility for old state defines


Referring Env variable in your code for hyperlink (Usage Example)
------------------------------------------------------------------

Your documentation text while using/referring env like
this ``:envvar:`RADICAL_TEST_ENV``` and continuing.