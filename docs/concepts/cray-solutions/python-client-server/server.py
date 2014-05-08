#!/usr/bin/python

import sys,os,time
import socket,select

# here specify the pbs script for target machine
qsubString = 'qsub pbs-kraken.py'
#qsubString = 'qsub pbs-hector.py'

#####################################################
# THIS NEEDS TO BE CHANGED FOR A SPECIFIC PATH!!!
#####################################################
exe_path = "/home/e290/e290/antonst/triplex/exam"
#####################################################

# specifying total number of runs to perform 
totalRuns = 32

# number of tasks to execute simultaneously
# depends on the number of cores per node
tasksPerNode = 2

HOSTNAME = socket.gethostname()
HOST = socket.gethostbyname(HOSTNAME)
PORT = 8000

#####################################################
# opening client.py file in order to pass IP address
# i know not very elegant but works
#####################################################
name = 'client.py'

try:
    rfile = open(name,'r')
except IOError:
    print ('Warning: unable to access file %s')%name

tbuffer = rfile.read()
rfile.close()

tbuffer = tbuffer.replace("@server@",str(HOST))

try:
    wfile = open(name,'w')
except IOError:
    print ('Warning: unable to access file %s')%name

wfile.write(tbuffer)
wfile.close()
#####################################################

taskString = exe_path

# specifying how many tasks to execute simultaneously on one node 
# it is not possible to pass list or a dictionary, so we are sending a string
for n in range(tasksPerNode-1):
    taskString = taskString + ' ' + exe_path

finishString = 'STOP'

os.system(qsubString)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))

server.listen(32)
print 'Login-node started listening at: %s' % HOST

input = [server,]

while (totalRuns > 0):
    inputready,outputready,exceptready = select.select(input,[],[])

    for s in inputready:
        if s == server:
            # handle the server socket 
            client, address = server.accept()
            input.append(client)
            print 'new client added %s'%str(address)
        else:
            # handle all other sockets 
            data = s.recv(1024)
            print 'Received: ', repr(data)
            if (totalRuns > 0):
                s.sendall(taskString)
                totalRuns -= 1
            else:
                s.sendall(finishString)  
                s.close()
                input.remove(s)
server.close()


