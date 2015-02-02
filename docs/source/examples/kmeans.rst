.. _chapter_example_kmeans:

******************
K-Means Clustering
******************

Introduction
------------

This example implements the k-means algorithm using the RADICAL-Pilot API.

Obtaining the code
------------------


To download the source files of k-means algorithm::

    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/kmeans/k-means.py
    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/kmeans/clustering_the_elements.py
    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/kmeans/finding_the_new_centroids.py

And to download an example dataset::

    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/kmeans/dataset4.in


Running the example
-------------------

To give it a test drive try via command line the following command::

    python k-means.py 3

where 3 is the number of clusters the user wants to create.


More About the Algorithm
------------------------

This application creates the clusters of the elements found in the dataset4.in
file. You can create your own file or create a new dataset file using the
following generator::

    curl --insecure -Os https://raw.githubusercontent.com/radical-cybertools/radical.pilot/readthedocs/examples/kmeans/creating_dataset.py

Run via command line::

        python creating_dataset.py <number_of_elements>

The algorithm takes the elements from the dataset4.in file. Then, it chooses
the first k centroids using the quickselect algorithm. It divides into
number_of_cores files the initial file and pass each file as an argument to
each Compute Unit. Every Compute Unit find in which cluster every element
belongs to and creates k different sums of the elements coordinates. and
returns this sum. Then, we sum all the sums of the CUs, and find the average
elements who are closest to the new centroids. Afterwards, we do the same
decomposition, but this time we try to find the new centroids. From each CU
we find the nearest element to each centroid, and return them to the main
program. Then we compare the results of all the CUs, and we decided who are
the new centroids. If we have convergence we stop the algorithm, otherwise we
start a new iteration.
