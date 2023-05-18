.. _chapter_tutorials:

=========
Tutorials
=========

This set of tutorials explore some of RADICAL-Pilot capabilities. Each tutorial
focuses on a specific topic, covering simple and more advanced details. Before
dwelling into these tutorials, you should be comfortable with our
`getting started <getting_started.ipynb>`_ example application.


Running the Tutorials
---------------------

Tutorials can be run via our self-contained Docker container or independently.
To run the tutorials in our Docker container:

1. clone the tutorials repository:

  ```shell
  git clone git@github.com:radical-cybertools/tutorials.git
  ```

2. Follow the instructions in the `README.md
   <https://github.com/radical-cybertools/tutorials/blob/main/README.md>`_,
   choosing method A or B.
3. After following the instructions, you will be given a URI to cut and paste in
   your browser to access to the Jupyter Notebook server that is running in the
   container.
4. Load and execute each tutorial in the Jupyter Notebook server on your
   browser.
5. Once finished, stop all the containers you started to execute the tutorial.



List of Tutorials
-----------------

.. toctree::
   :maxdepth: 2
   :glob:

   tutorials/*