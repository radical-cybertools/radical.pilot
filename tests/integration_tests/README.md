# Integration testing with resources

Integration testing on resources requires periodic jobs. To achieve this, batch scripts are offered under the `batch_scripts` folder. Currently, there is a script for SDSC Comet.

The script defines the following to execute the test:

- A `TEST` environment variable that contains the path to the test files to be executed. All test paths should be included and be space-separated.
    Example:
    ```
    TEST="radical.pilot/tests/integration_tests/test_resources/test_rm/test_slurm.py"
    ```

- Moves to the assigned folder for testing, which is the same as submitting the script, removes any prior copy of RP's repo, the previous virtual environment variable, and clones the `devel` branch only.

- Loads the necessary modules, creates and activates a virtual environment. Those lines should be updated for every resource. After the virtual environment's activation, the script installs RP and runs the tests. If one or more or those tests fail, the script runs a utility python file that creates an RP repository issue. This python file takes as arguments the name of the resource and the log filename. Update the resource name accordingly.

- Finally, the script submits another job to the queue.

## Setup

Tests require that a Github token is set up and included in the `.bashrc` of the account that runs the tests.

Example:
```
export GIT_TOKEN=soimethignsomething
```

[![Summit Integration Tests](https://github.com/iparask/git_actions/actions/workflows/summit.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/summit.yml)
[![Bridges2 Integration Tests](https://github.com/iparask/git_actions/actions/workflows/bridges.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/bridges.yml)
[![Comet Integration Tests](https://github.com/iparask/git_actions/actions/workflows/comet.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/comet.yml)
