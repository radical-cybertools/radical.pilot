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

- Send a singal to Github with the state of the test via a `curl` command. For example:
```bash
   curl -H "Accept: application/vnd.github.everest-preview+json" \
    -H "Authorization: token $GIT_TOKEN" \
    --request POST \
    --data '{"event_type": "test_<resource>", "client_payload": { "text": "<state>"}}' \
    https://api.github.com/repos/radical-cybertools/radical.pilot/dispatches
```
Please see existing batch scripts for the data specifics.

- Finally, the script submits another job to the queue.

## Setup

Tests require that a Github token is set up and included in the `.bashrc` of the account that runs the tests.

Example:
```
export GIT_TOKEN=soimethignsomething
```

## Create a Github workflow

The last part is to create a Github workflow that provides information about the state of the integration tests. This Github workflow file lives under `.github/workflows`.

The template of such a file is:
```yaml
name: Resource Integration Test

on:
  repository_dispatch:
    types: [ test_<resource> ]
  schedule:
    - cron: '0 0 1 */1 *'

jobs:
  inter_jobs:
    runs-on: ubuntu-latest
    steps:
    - name: Check event
      if: github.event.client_payload.text
      run: |
        echo "Event text: ${{ github.event.client_payload.text }}"
        if [ ${{ github.event.client_payload.text }} == "success" ]
        then
            echo "Success"
        else
            echo "Failure"
            false
        fi
  periodic_jobs:
    if: github.event.client_payload.text == null
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python 3.6
      uses: actions/setup-python@v2
      with:
        python-version: 3.6.13
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install requests
    - name: Cron
      run: |
        tests/bin/radical-pilot-test-verify -y '<filename>' -d 7

```

This file receives a signal from outside, checks the payload and succeeds or fails based on the payload. Also, it defines a monthly cron job that checks if it received a signal the last week. If not, it fails the test for the specific resource.

## Add a badge

It is necessary to add a badge in the main README file of RP for a resource where integration tests are running. The badge looks like:
```
[![Resource Integration Tests](https://github.com/radical-cybertools/radical.pilot/actions/workflows/<filename>.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/<filename>.yml)
```
