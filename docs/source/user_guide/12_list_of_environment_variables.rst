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
    * - .. envvar:: RADICAL_DEFAULT_LOG_TGT
      - dsa
      - defd
    * - RADICAL_DEFAULT_LOG_DIR
      - Log dir
      - $PWD
    * - RADICAL_DEFAULT_LOG_LVL
      - ERROR
      - ERROR
    * - RADICAL_DEFAULT_REPORT
      - TRUE
      - TRUE
    * - RADICAL_DEFAULT_REPORT_TGT
      - stderr
      - stderr
    * - RADICAL_DEFAULT_REPORT_DIR
      - Directory of Reporter module
      - $PWD
    * - RADICAL_DEFAULT_PROFILE
      - True
      - TRUE
    * - RADICAL_DEFAULT_PROFILE_DIR
      - $PWD
      - PWD

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
    * - .. envvar::


Referring Env variable in your code for hyperlink (Usage Example)
------------------------------------------------------------------

Your documentation text while using/referring env like
this ``:envvar:`RADICAL_TEST_ENV``` and continuing.