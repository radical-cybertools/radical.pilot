Referring Env variable in your code for hyperlink (Usage Example)
------------------------------------------------------------------

Your documentation text while using/referring env like
this ``:envvar:`RADICAL_TEST_ENV``` and continuing.

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
      - Cancel the tasks immediately. #TODO Didn't understood
      - {NOT_SET}
    * - .. envvar:: RADICAL_SAGA_PTY_SSH_TIMEOUT
      - Sets the connection timeout (in seconds, default: 10) for the SAGA PTY layer. On connection timeouts should be set to 60 or more
      - `10.0`
    * - .. envvar:: RADICAL_DEFAULT_LOG_TGT
      - dsa
      - defd
    * - .. envvar:: RADICAL_DEFAULT_LOG_DIR
      - Log dir
      - $PWD
    * - .. envvar:: RADICAL_DEFAULT_LOG_LVL
      - ERROR
      - ERROR
    * - .. envvar:: RADICAL_DEFAULT_REPORT
      - TRUE
      - TRUE
    * - .. envvar:: RADICAL_DEFAULT_REPORT_TGT
      - stderr
      - stderr
    * - .. envvar:: RADICAL_DEFAULT_REPORT_DIR
      - Directory of Reporter module
      - $PWD
    * - .. envvar:: RADICAL_DEFAULT_PROFILE
      - True
      - TRUE
    * - .. envvar:: RADICAL_DEFAULT_PROFILE_DIR
      - $PWD
      - PWD

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
      - Used for log level
      - `#TODO`
    * - <NS>_LOG_TGT
      - Used for the log targets
      - #TODO

.. note:: The name space is used to derive env variable names for log levels and targets. If no ns is given, the ns is derived from the name. Eg. the name `radical.pilot` becomes `RADICAL_PILOT`.

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
      - Where to report to
      - #TODO

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
      - related to :envvar:`RADICAL_DEBUG`, enables verbose messages for debugging.
      - #TODO
    * - *_PROFILE
      - `Profiler` is similar to `Logger` and `Reporter`
      - {NOT_SET}
    * - .. envvar:: RADICAL_PILOT_PRUN_VERBOSE
      - Increase verbosity of prun output
      - FALSE
    * - .. envvar:: UMS_OMPIX_PRRTE_DIR
      - #TODO Not understood
      - #TODO
    * - .. envvar:: RADICAL_SAGA_SMT
      - Sets SMT settings on some resources. Usually covered via resource config options
      - 1
    * - .. envvar:: RP_PROF_DEBUG
      - enable additional debug messages on profile extraction
      - {NOT_SET}

.. raw:: html

   <hr>

.. list-table:: Deprecated / Being Phased Out / No Longer valid
    :widths: 35 100
    :header-rows: 1

    * - Environment Variable Name
      - Description
    * - .. envvar:: RP_ENABLE_OLD_DEFINES
      - enable backward compatibility for old state defines
