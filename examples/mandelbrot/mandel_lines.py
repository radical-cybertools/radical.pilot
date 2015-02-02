__author__    = "George Chantzialexiou"
__copyright__ = "Copyright 2012-2013, The RADICAL Group"
__license__   = "MIT"

""" A Simple Mandelbrot Fractal Generator.

    We use this example to explore the distributed capabilities of
    the Pilot Job and Filesystem APIs. The mandelbrot module
    calculates  partial mandelbrot set fractal and writes it to a 
    PNG image file. To determine which part of the fractal we create
    we use the iterations variable.

    It requires the Python Image Library (PIL) which can be easily
    installed with 'pip install Pillow'.

    This program is required by the mandelbrot_pilot.py program
    which creates the mandelbrot fractal using the capabilities of
    Pilot Job API. 

    On the command line:

        python mandelbrot.py imgX imgY xBeg xEnd yBeg yEnd cores iterations

    The parameters are as follows:

        imgX, imgY: the dimensions of the mandelbrot image, e.g. 1024, 1024
        xBeg, xEnd: the x-axis portion of the (sub-)image to calculate
        yBeg, yEnd: the y-axis portion of the (sub-)image to calculate
        cores : the number of the system's cores
        iterations: the part of the image we create. Can vary from 1 to cores.
"""

import sys
from PIL import Image

################################################################################
##
def makemandel(mandelx, mandely, xbeg, xend, ybeg, yend, cores, iterations):

    # drawing area (xa < xb and ya < yb)
    xa = -2.0
    xb =  1.0
    ya = -1.5
    yb =  1.5

    # maximum iterations
    maxIt = 128 
    # the output image
    image = Image.new("RGB", (xend-xbeg, yend-ybeg))
    ybeg2 = int(((yend*(iterations-1))/cores))
    yend2 = int(((yend*(iterations))/cores))

    for y in range(ybeg2, yend2):
        cy = y * (yb - ya) / (mandely - 1)  +  ya
        for x in range(xbeg, xend):
            cx = x * (xb - xa) / (mandelx - 1) + xa
            c = complex(cx, cy)
            z = 0
            for i in range(maxIt):
                if abs(z) > 2.0: break 
                z = z * z + c 
            r = i % 4 * 16
            g = i % 6 * 16
            b = i % 16 * 16
            image.putpixel((x-xbeg, y-ybeg), b * 65536 + g * 256 + r)
 
    image.save('mandel_%d.gif' % iterations , "GIF")
    
    return image

################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]
    
    imgX = int(sys.argv[1])
    imgY = int(sys.argv[2])
    xBeg = int(sys.argv[3])
    xEnd = int(sys.argv[4])
    yBeg = int(sys.argv[5])
    yEnd = int(sys.argv[6])
    cores = int(sys.argv[7])
    iterations = int(sys.argv[8])

    makemandel(imgX, imgY, xBeg, xEnd, yBeg, yEnd, cores, iterations)
    sys.exit(0)
