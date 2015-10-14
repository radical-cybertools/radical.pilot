.. _chapter_example_mandelbrot:

**************
Mandelbrot set
**************

Requirements
------------

This Mandelbrot example needs the PIL library for both the "application side" and the "CU side".
For the application side you need to install the Pillow module in the same virtual environment as you have installed RADICAL-Pilot into::

    pip install Pillow

The examples are constructed in such a way that PIL is dynamically installed in the CU environment; more on that later.

Obtaining the code
------------------

Download the mandelbrot example via command line::

    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/mandelbrot/mandelbrot_pilot_cores.py
    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/mandelbrot/mandel_lines.py

Customizing the example
-----------------------

Open the file mandelbrot_pilot_cores.py with your favorite editor.
There is a critical section that must be filled in by the user.
About halfway of this file it says, "BEGIN REQUIRED CU SETUP."
This section defines the actual tasks to be executed by the pilot.

Let's discuss the above example.
We define our executable as "python".
Next, we need to provide the arguments.
In this case, mandel_lines.py is the python script that creates parts of the mandelbrot fractal.
The other arguments are the variables that the mandel_lines.py program needs in order to be executed.
Note that this block of code is in a python for loop, therefore, e.g. "i" corresponds to what iteration we are on.
This is  a parallel code, the python uses as many cores as we define,
(now we defined cores=4) to create smaller parts of the fractal simultaneously.


More About the Algorithm
------------------------

This algorithm takes the takes the parameters of the Mandelbrot fractal and decompose the image into n different parts, where n is the number of the cores of the system. Then it runs for every part the mandelbrot Generator Code  which is the mandel_lines.py. The mandel_lines.py creates n Images and then we compose the n images into one. The whole fractal Image. For every part of the image we create one Compute Unit.


Run the example
---------------

Save the file and executed::

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
