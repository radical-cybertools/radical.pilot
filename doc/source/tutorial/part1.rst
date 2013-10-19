
Part 1: Introduction
********************

Sinon provides ...

In part 2 of this tutorial, we will ...
In part 3, we ...


Installation
============

.. warning:: Sinon requires **Python >= 2.5**. It won't work with an older version of Python.

Install Virtualenv
------------------

A small Python command-line tool called `virtualenv <http://www.python.org/>`_
allows you to create a local Python environment (sandbox) in user space, which
allows you to install additional Python packages without having to be 'root'.

To create your local Python environment run the following command (you can install virtualenv on most systems via apt-get or yum, etc.):

.. code-block:: bash

   virtualenv $HOME/tutorial

If you don't have virtualenv installed and you don't have root access on your
machine, you can use the following script instead:

.. code-block:: bash

   curl --insecure -s https://raw.github.com/pypa/virtualenv/master/virtualenv.py | python - $HOME/tutorial

.. note:: If you have multiple Python versions installed on your system, you can
   use the ``virtualenv --python=PYTHON_EXE`` flag to force virtualenv to use
   a specific version.

Next, you need to activate your Python environment in order to make it work:

.. code-block:: bash

   source $HOME/tutorial/bin/activate

Activating the virtualenv is very important. If you don't activate your
virtualenv, the rest of this tutorial will not work. You can usually tell that
your environment is activated properly if your bash command-line prompt starts
with ``(tutorial)``.


Install Sinon
-------------

The latest sinon module is available via the `Python Package Index
<https://pypi.python.org/pypi/sinon>`_  (PyPi). PyPi packages are installed very
similar to Linux deb or rpm packages with a tool called ``pip`` (which stands
for "pip installs packages"). Pip is installed by default in your virtualenv, so
in order to install Sinon, the only thing you have to do is this:

.. code-block:: bash

   pip install sinon

You will see some downloading and unpacking action and if everything worked ok, the last two lines should look like this:

.. code-block:: none

   Finished processing dependencies for sinon==0.9.3

To make sure that your installation works, run the following command to check if
the sinon module can be imported by the interpreter (the output of the command
below should be version number of the sinon module):

.. code-block:: bash

   python -c "import sinon; print sinon.version"

