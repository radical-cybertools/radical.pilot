# JSRUN test suite

## Requirements

* Every individual test needs to be placed in ```test_*``` folder;
* three files are required under the ```test_*``` folder:
  * ```cmds```:
    * line 1: short description about test;
    * line 2: any pre_exec cmds;
    * line 3: jsrun cmd to execute;
  * ```res_set```: Resource set file for the test;
  * ```exp_out```: expected output;
* assertion are exact: check for stray spaces, linebreaks, etc.

## Execution

To manually execute all the tests, clone the ```test_jsrun``` folder on Summit
and run ```bsub jobs.sh```. All the tests are performed daily via ```at``` on
Summit. See inline comments for details.

### Note

* Base directory is ```/ccs/home/vivekb/test_jsrun```;
* results are located in a folder named with the current date;
* a summary is sent to the email address specified in ```jobs.sh```.

## Misc

* The ```at``` process needs to be reinvoked after system maintenenace.
