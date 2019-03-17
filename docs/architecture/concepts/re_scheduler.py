#!/usr/bin/env python

import time
import regex

cpu_procs   = 2
cpu_threads = 3
gpu_procs   = 2
gpu_threads = 3

cross_sockets = False  # can threads   cross sockets?
cross_nodes   = False  # can processes cross nodes?


# In the general case, we search for a mix of GPUs and CPUs, with the
# following constraints: 
#
#   - CPU cores for threads which belong to the same process need to be near
#     each other  (on the same node or even the same core)
#   - GPUs need to be near to the processes usinng them  (on the same node or
#     even the same socket).
#
# FIXME: do we need to introduce an additional hierarchy level: 
#        node > socket > core > hw_thread ?

# use different characters for resource types and delimiters
p_g0 = '\-'  # free GPU
p_g1 = '\+'  # used GPU
p_c0 = '_'   # free CPU
p_c1 = '#'   # used CPU
p_s  = ':'   # socket boundary
p_n  = '\|'  # node   boundary

# threads need a sequence of cores which never cross node boundaries, but may
# also not even cross socket boundaries
if cross_sockets: p_tseq = '[^%s]'   % (p_n)
else            : p_tseq = '[^%s%s]' % (p_n, p_s)

# processes need a sequence or cores which may or may not be able to cross
# node boundaries.
if cross_nodes  : p_pseq = ''
else            : p_pseq = '[^%s]' % (p_n)

# gpu sequences have the same constrains as process sequences
if cross_nodes  : p_gseq = ''
else            : p_gseq = '[^%s]' % (p_n)


# When searching for matching nodes, we first do a look-ahead (?=) to see if
# a node has at least one chunk of cores (chunk: process + threads), or a GPU,
# or both.  In case of oversubscription, a GPU also needs a core in addition to
# the chunk.
p_t       =  '(?:%s%s*?){%d}'     % (p_c0, p_tseq, cpu_threads)
p_c       =  '((?:%s%s*?){%d})'   % (p_t,  p_pseq, cpu_procs)
p_g       =  '((?:%s%s*?){%d})'   % (p_g0, p_gseq, gpu_procs)
pattern   = r'((?=%s)%s)'         % (p_c,  p_g)
# pattern   = r'(%s)'             % (p_c)

nodes     = '' \
          + '| _###_###### +-+ : #__###__##_ -++ ' \
          + '| #######_#__ +++ : _##__##_### ++- ' \
          + '| #####_#_##_ --+ : ##_##__#### +-+ ' \
          + '|'

tested = '((?=((?:(?:_[^:\|]*?){3})[^\|]*?){2})(?:(?:.*?(?:-)){2}))'
print tested
print pattern
print nodes
print

# https://regex101.com/
#
#   ((?=((?:(?:_[^:\|]*?){3})[^\|]*?){2})(?:(?:.*?(?:-)){2}))'
#
#   | _###_###### +-+ : #__###__##_ -++ 
#   | #######_#__ +++ : _##__##_### ++- 
#   | #####_#_##_ --+ : ##_##__#### +-+ |
#         [^^^^^^ ^^]    [^^^^^]
#
#   Full match	81-90	_#_##_ --
#   Group 1.	81-90	_#_##_ --
#   Group 2.	96-101	_##__ 


pattern = tested
start = time.time()
pos = 0
cnt = 0
while True:
    match = regex.search(pattern, nodes, pos=pos)
    if not match:
        break
    alloc = match.group(0)
    print '%-5d  %-5d ||  %30s  || -  %s cores  %s gpus' \
          % (pos, match.start(0), alloc, alloc.count('_'), alloc.count('-'))
    pos  = match.end(0)
    try:
        caps = match.captures()
        for tmp in caps:
            print '             ||  %30s  || -  %s cores  %s gpus' \
                  % (tmp, tmp.count('_'), tmp.count('-'))
    except:
        pass
    print
    cnt += 1

stop = time.time()
print cnt
print '%.2f' % (cnt / (stop - start))


