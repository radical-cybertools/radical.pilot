List of Env vars
_________________

.. list-table:: User List
   :widths: 35 100 20 20
   :header-rows: 1

   * - Environment Variable Name
     - Description
     - Default value
     - Is_deprecated
     - Importance
   * - .. envvar:: RADICAL_UTILS_NO_ATFORK
     - Monkeypatching can be disabled by setting RADICAL_UTILS_NO_ATFORK
     - N
     - LOW


.. list-table:: Developer List
   :widths: 35 100 20 20
   :header-rows: 1

   * - Environment Variable Name
     - Description
     - Default value
     - Is_deprecated
     - Importance
     -
   * -    "log_lvl"    : "${RADICAL_DEFAULT_LOG_LVL:ERROR}",
    "log_tgt"    : "${RADICAL_DEFAULT_LOG_TGT:.}",
    "log_dir"    : "${RADICAL_DEFAULT_LOG_DIR:$PWD}",
    "report"     : "${RADICAL_DEFAULT_REPORT:TRUE}",
    "report_tgt" : "${RADICAL_DEFAULT_REPORT_TGT:stderr}",
    "report_dir" : "${RADICAL_DEFAULT_REPORT_DIR:$PWD}",
    "profile"    : "${RADICAL_DEFAULT_PROFILE:TRUE}",
    "profile_dir": "${RADICAL_DEFAULT_PROFILE_DIR:$PWD}"

   * - .. envvar:: RADICAL_UTILS_NO_ATFORK
     - Monkeypatching can be disabled by setting RADICAL_UTILS_NO_ATFORK
     - N
     - LOW


Referring Env variable in your code for hyperlink (Usage Example)
----------------------------------------------------------------------

Your documentation text while using/referring env like
this ``:envvar:`RADICAL_TEST_ENV``` and continuing.