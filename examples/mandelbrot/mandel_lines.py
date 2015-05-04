#!/usr/bin/env python

__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


""" A Simple Mandelbrot Fractal Generator.

    We use this example to explore the distributed capabilities of
    the Pilot Job and Filesystem APIs. The mandelbrot module
    calculates  partial mandelbrot set fractal and writes it to a 
    PNG image file. To determine which part of the fractal we create
    we use the index variable.

    It requires the Python Image Library (PIL) which can be easily
    installed with 'pip install Pillow'.

    This program is required by the mandelbrot_pilot.py program
    which creates the mandelbrot fractal using the capabilities of
    Pilot Job API. 

    On the command line:

        python mandelbrot.py imgX imgY xBeg xEnd yBeg yEnd cores index

    The parameters are as follows:

        imgX, imgY: the dimensions of the mandelbrot image, e.g. 1024, 1024
        xBeg, xEnd: the x-axis portion of the (sub-)image to calculate
        yBeg, yEnd: the y-axis portion of the (sub-)image to calculate
        cores : the number of the system's cores
        index: the part of the image we create. Can vary from 1 to cores.
"""

import sys
from PIL import Image

# ------------------------------------------------------------------------------
#
def makemandel(x_pixel_max, y_pixel_max, x_real_beg, x_real_end, y_real_beg, y_real_end, cores, index):

    maxIt  = 128 
    image  = Image.new("RGB", (x_pixel_max, y_pixel_max))

    y_real_range   = y_real_end - y_real_beg
    y_real_slice   = float(y_real_range) / float(cores)
    y_real_offset  = float(y_real_slice) * float(index)

    y_pixel_slice  = int(y_pixel_max / cores)
    y_pixel_offset = y_pixel_slice * index

    x_real_range   = x_real_end - x_real_beg
    x_real_slice   = float(x_real_range)
    x_pixel_slice  = int(x_pixel_max)
    x_pixel_offset = 0

    for y in range(y_pixel_slice):
        cy = y_real_beg + y_real_offset + (y_real_slice/y_pixel_slice*y)
        for x in range(x_pixel_slice):
            cx = x_real_beg + (x_real_slice/x_pixel_slice*x)

            c = complex(cx, cy)
            z = 0
            for i in range(maxIt):
                if abs(z) > 2.0: break 
                z = z * z + c 
            r = i % 4 * 16
            g = i % 6 * 16
            b = i % 16 * 16
            image.putpixel((x, y_pixel_slice*index+y), b * 65536 + g * 256 + r)
 
    image.save('mandel_%d.gif' % index , "GIF")
    
    return image

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    args = sys.argv[1:]

    imgX  =   int(sys.argv[1])
    imgY  =   int(sys.argv[2])
    xBeg  = float(sys.argv[3])
    xEnd  = float(sys.argv[4])
    yBeg  = float(sys.argv[5])
    yEnd  = float(sys.argv[6])
    cores =   int(sys.argv[7])
    index =   int(sys.argv[8])

    makemandel(imgX, imgY, xBeg, xEnd, yBeg, yEnd, cores, index)
    sys.exit(0)


# ------------------------------------------------------------------------------

