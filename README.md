saga-pilot
==========

Installation from source
------------------------

In the top-level folder run:

    python setup.py install

Run Tests
---------

Most of the tests run locally, but some of the tests can be run on remote 
machines as well. The tests need the following environment variables to be 
set in order for that to work:

    SAGAPILOT_TEST_RESOURCE - The resource to run on (localhost)
    SAGAPILOT_TEST_WORKDIR - The working directory (/tmp/sinon.unit-tests)
    SAGAPILOT_TEST_CORES - Number of cores to allocate (1)
    SAGAPILOT_TEST_NUM_CUS - Number of cus to launch (1)


The values in parenthesis are the default values if the environment variables 
remain unset. 

To run the unit tests, type the following command in the source root:

    python setup.py test
