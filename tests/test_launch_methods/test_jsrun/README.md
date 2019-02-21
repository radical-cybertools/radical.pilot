# JSRUN test suite

## Requirements

* Every individual test needs to be placed in test_* folder
* Three files are required under the test_* folder:
  * 'cmds'
    * line 1: short description about test
    * line 2: any pre_exec cmds
    * line 3: jsrun cmd to execute
  * 'res_set': Resource set file for the test
  * 'exp_out': expected output
    * Note: assertion will be exact (check for stray spaces, linebreaks)

## Execution

To execute the all the tests, clone the test_jsrun folder on Summit
and run ```bsub jobs.sh```. This automatically done by an ```at``` process
on Summit. See inline comments for details.

### Note

* Results are located in a folder with the current date.
* A summary is sent to the mailing list specified in jobs.sh. 

## Misc

* The ```at``` process needs to be reinvoked after system maintenenace.