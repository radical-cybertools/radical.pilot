
############
Installation
############


Requirements
------------

Sinon has the following requirements:

* Python 2.5 or newer
* SAGA-Python


Installation via PyPi
---------------------

Sinon is available for download via PyPI and may be installed using 
easy_install or pip (preferred). Both automatically download and install 
all dependencies required by Sinon if they can't be found on your system:

.. code-block:: bash

    pip install sinon


or with easy_install:

.. code-block:: bash

    easy_install sinon


Using Virtualenv
----------------

If you don't want to (or can't) install Sinon into your system's Python 
environment, there's a simple (and often preferred) way to create an 
alternative Python environment (e.g., in your home directory): 

.. code-block:: bash

    virtualenv $HOME/ve/
    . $HOME/ve/bin/activate
    pip install sinon


**What if my system Doesn't come With virtualenv, pip or easy_install?**

There's a simple workaround for that using the 'instant' version of virtualenv.
It also installs easy_install and pip:

.. code-block:: bash

    curl --insecure -s https://raw.github.com/pypa/virtualenv/1.9.X/virtualenv.py | python - $HOME/ve
    . $HOME/ve/bin/activate
    pip install sinon


Installing the Latest Development Version
-----------------------------------------

.. warning:: Please keep in mind that the latest development version of Sinon
can be highly unstable or even completely broken. It's not recommended to use it
in a production environment.

You can install the latest development version of Sinon directly from our Git
repository using pip:

.. code-block:: bash

    pip install -e git://github.com/saga-project/saga-pilot.git@devel#egg=sinon

