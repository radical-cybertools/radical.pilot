__author__ = 'vivek'
import os
import sys
from extasy import script

if __name__ == '__main__':
    print 'creating new crd files...'
    nreps = int(sys.argv[1])
    cycle = int(sys.argv[2])
    dict = {}
    dict['cycle'] = cycle
    for rep in range(nreps):
        tl = script.Script()
        tl.append('source leaprc.ff99SB')
        tl.append('x = loadpdb pdbs/%s.pdb'%(rep))
        tl.append('saveamberparm x delete.me min%s%s.crd'%(cycle,rep))
        tl.append('quit')
        tl.run('tleap -f {}')
