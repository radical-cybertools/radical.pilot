.. _chapter_example_mandelbrot:

**************
Mandelbrot set
**************

Requirements
------------

In order to make this example work, we need first to activate your virtualenv::

    source $HOME/myenv/bin/activate

And install the PIL module::

    pip install Pillow

The simplest usage of a pilot-job system is to submit multiple identical tasks (a ‘Bag of Tasks’) collectively, i.e. as one big job! Such usage arises for example to perform either a parameter sweep job or a set of ensemble simulation.

We will create an example which submits N jobs using RADICAL-Pilot. The jobs are all identical, except that they each record their number in their output. This type of run is very useful if you are running many jobs using the same executable (but perhaps with different input files). Rather than submit each job individually to the queuing system and then wait for every job to become active and complete, you submit just one container job (called a Pilot) that reserves the number of cores needed to run all of your jobs. When this pilot becomes active, your tasks (which are named ‘Compute Units’ or ‘CUs’) are pulled by RADICAL-Pilot from the MongoDB server and executed.

Download the mandelbrot via command line::

    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/mandelbrot/mandelbrot_pilot_cores.py
    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/mandelbrot/mandel_lines.py

Customizing the example
-----------------------

Open the file mandelbrot_pilot_cores.py with your favorite editor. There is a critical sections that must be filled in by the user: Line 157 of this file says, “BEGIN REQUIRED CU SETUP.” This section defines the actual tasks to be executed by the pilot.

Let’s discuss the above example. We define our executable as “python” , which is the python module. Next, we need to provide the arguments. In this case,
mandel_lines.py is the executable that creates parts of the mandelbrot fractal. The other arguments are the variables that the mandel_lines.py program needs in order to be executed. .These arguments are  environment variables, so we will need to provide a value for it, as is done on the next line: 
{"mandelx": "%d" % imgX, "mandely": "%d" % imgY, "xBeg": "%d" % xBeg, "xEnd": "%d" % xEnd,  "yBeg": "%d" % yBeg,   "yEnd": "%d" % yEnd, "cores": "%d" % pdesc.cores, "iter": "%d" % i }. Note that this block of code is in a python for loop, therefore,e.g. i corresponds to what iteration we are on. This is  a parallel code, the python uses as many cores as we define, ( now we defined cores=4) to create smaller parts of the fractal simultaneously. 


More About the Algorithm
------------------------

This algorithm takes the takes the parameters of the Mandelbrot fractal and decompose the image into n different parts, where n is the number of the cores of the system. Then it runs for every part the mandelbrot Generator Code  which is the mandel_lines.py. The mandel_lines.py creates n Images and then we compose the n images into one. The whole fractal Image. For every part of the image we create one Compute Unit.


Run the example
---------------

Save the file and executed:

    python mandelbrot_pilot_cores.py 1024 1024 0 1024 0 1024

The parameters are the following: imgX, imgY: the dimensions of the mandelbrot image, e.g. 1024, 1024 xBeg, xEnd: the x-axis portion of the (sub-)image to calculate yBeg, yEnd: the y-axis portion of the (sub-)image to calculate

The output should look something like this::

    Initializing Pilot Manager ...
    Submitting Compute Pilot to Pilot Manager ...
    Initializing Unit Manager ...
    Registering Compute Pilot with Unit Manager ...
    Submit Compute Units to Unit Manager ...
    Waiting for CUs to complete ...
    ...

    All Compute Units completed successfully! Now..
    Stitching together the whole fractal to : mandelbrot_full.gif
    Images is now saved at the working directory..
    Session closed, exiting now ...

When you finish the execution you may find the image in your working directory: mandelbrot_full.gif

.. image:: https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/docs/source/images/mandelbrot_full.gif
