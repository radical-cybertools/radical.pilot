#!/usr/bin/env python

__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2014, The RADICAL Group"
__license__   = "MIT"

import os
import sys
import math

#------------------------------------------------------------------------------
#
def get_distance(dataPointX, dataPointY, centroidX, centroidY):
    
    # Calculate Euclidean distance.
    return math.sqrt(math.pow((centroidY - dataPointY), 2) + math.pow((centroidX - dataPointX), 2))

################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]
    
    cu = int(sys.argv[1])   
    k  = int(sys.argv[2])
    
    # READING THE CENTROIDS FILE
    centroid = []
    data = open("centroids.txt", "r")
    read_as_string_array = data.readline().split(',')
    centroid = map(float, read_as_string_array)
    data.close()
    #print centroid
    # END OF READING THE CENTROIDS FILE


    # READING THE CU FILE - WHICH IS THE ELEMENT FILE 
    elements = []
    read_file = open('cu_%d.data' % cu, 'r')
    read_as_string_array = read_file.readline().split(',')
    elements = map(float, read_as_string_array)
    #print elements
    # END OF READING THE CU FILE



    # FIND THE NUMBER AND THE SUM OF THE ELEMENTS OF EACH CENTROID
    sum_of_centroids = []
    for i in range(0,2*k):
        sum_of_centroids.append(0)

    # here we classify in which centroid each element belongs to and add it to the sum of the coordinates of each centroid
    # to the sum_of_centroids list. And keep the number of the elements that belong to each centroid.
    # The right element is the one that is closer to the centroid using the Euclidean distance function
    for i in range(0,len(elements)):
        El = get_distance(elements[i],0,centroid[0],0)   # i can change this because i only have one variable
        index = 0  # that means that this is the first centroid
        for j in range(1,k):
            El2 = get_distance(elements[i],0,centroid[j],0)
            if (El != min(El,El2)): 
                index = j
                El = El2 
        sum_of_centroids[2*index] += elements[i]
        sum_of_centroids[(2*index)+1] += 1  

    file_open = open('centroid_cu_%d.data' % cu, 'w')

    # END OF FINDING THE NUMBER AND THE SUM OF THE ELEMENTS OF EACH CENTROID

    # WRITING THE FILES TO A FILE
    out_file = open('centroid_cu_%d.data' % cu, 'w')
    centroid_to_string = ','.join(map(str,sum_of_centroids))
    out_file.write(centroid_to_string)
    out_file.close() 

    # END OF WRITING THE FILES TO A FILE


    sys.exit(0)


# ------------------------------------------------------------------------------

