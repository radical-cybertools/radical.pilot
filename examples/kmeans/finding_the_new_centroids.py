__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2014, The RADICAL Group"
__license__   = "MIT"

import os, sys, math

def get_distance(dataPointX, dataPointY, centroidX, centroidY):

    # Calculate Euclidean distance.
    return math.sqrt(math.pow((centroidY - dataPointY), 2) + math.pow((centroidX - dataPointX), 2))
#------------------------------------------------------------------------------
#

################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]

    cu = int(sys.argv[1])
    k = int(sys.argv[2])

    #----------------------READING THE CENTROIDS FILE
    centroid = []  # the centroids files have the average elements
    data = open("centroids.txt", "r")
    read_as_string_array = data.readline().split(',')
    centroid = map(float, read_as_string_array)
    data.close()
    #print centroid
    #--------------------END OF READING THE CENTROIDS FILE


    #-----------------READING THE CU FILE - WHICH IS THE ELEMENT FILE 
    elements = []
    read_file = open('cu_%d.data' % cu, 'r')
    read_as_string_array = read_file.readline().split(',')
    elements = map(float, read_as_string_array)
    #print elements
    #--------------------END OF READING THE CU FILE


   #------------FINDING THE ELEMENT WHICH IS CLOSEST TO THE AVG_ELEMENT

    new_centroids = []
    a = sys.maxint
    for i in range(0,k):
        new_centroids.append(a)

    for i in range(0,len(elements)):
        El = get_distance(elements[i],0,centroid[0],0)
        index = 0  # that means that this is the first centroid
        for j in range(1,k):
            El2 = get_distance(elements[i],0,centroid[j],0)
            if (El != min(El,El2)):
                index = j
                El = El2
            if (get_distance(new_centroids[index],0,centroid[j],0)>El):
                new_centroids[index] = elements[i]

        # now at new_centroids_list i have the candidate centroids. We can decide after the compotition


    #-----------END OF FINDING THE ELEMENT WHICH IS CLOSEST TO THE AVG_ELEMENT




    #-----------------WRITING THE FILES TO A FILE
    out_file = open('centroid_cu_%d.data' % cu, 'w')
    centroid_to_string = ','.join(map(str,new_centroids))
    out_file.write(centroid_to_string)
    out_file.close()

    #--------------END OF WRITING THE FILES TO A FILE


    sys.exit(0)


# ------------------------------------------------------------------------------

