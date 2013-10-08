#!/usr/bin/env python

#Specify this:
username = 'marius'


"""
Get an IPython notebook running on Carver. 

Usage:

ipyn.carv --dir /some/path --port ####
"""

from subprocess import Popen, PIPE, call
import sys
import webbrowser
from getopt import getopt
import time


args = dict(getopt(sys.argv[1:],[],['port=','dir=','inline=','usplanck=','reconnect=', 'kill='])[0])

port = args.get('--port',8888)
dir = args.get('--dir','')
inline = (args.get('--inline','True')=='True')
usplanck = (args.get('--usplanck','True')=='True')
reconnect = args.get('--reconnect',None)
kill = args.get('--kill',None)

def readwhile(stream,func):
    while True:
        line = stream.stdout.readline()
        if line!='':
            print line[:-1]
            if func(line): return line.strip()
        else:
            raise Exception("Disconnected unexpectedly.") 
            
def write(stream,line):
    print line
    stream.stdin.write(line)
    

def dotunnel(hostname,port):
    tunnel = ['ssh','-4', '-t', '-Y', 'carver.nersc.gov', '-L', '%s:localhost:%s'%(port,port), 'ssh', '-t', '-Y', hostname, '-L', '%s:localhost:%s'%(port,port)]
    print ' '.join(tunnel)
    ptunnel = Popen(tunnel,stdout=PIPE,stdin=PIPE)
    write(ptunnel,'echo TUNNEL\n')
    readwhile(ptunnel,lambda line: line.startswith('TUNNEL'))
    webbrowser.open('http://localhost:%s'%port)
    
if reconnect is None or kill is not None:        

    pqsub=Popen(['ssh','-t','-t','-4','%s@carvergrid.nersc.gov'%username],stdin=PIPE,stdout=PIPE,stderr=PIPE)

    if kill is not None:
        write(pqsub,'qdel %s && echo KILLED\n'%kill)
        readwhile(pqsub,lambda line: 'KILLED' in line)
        print "Killed notebook succesfully."
        sys.exit()
    
    cmd = ('echo HOSTNAME=\\`hostname\\` && '
           'cd %s &&'
           'ipython notebook --pylab%s --port=%s\n')%(dir,'=inline' if inline else '',port)
    write(pqsub,'cd .ipyn_running \n')
    write(pqsub,'echo "%s" | qsub -V -j oe '%cmd)
    if usplanck: write(pqsub,'-q usplanck -l nodes=1:ppn=1,pvmem=20gb -l walltime=12:00:00')
    write(pqsub,' - \n')

    job = readwhile(pqsub, lambda line: 'cvr' in line)
    write(pqsub,'tail -f --retry %s.OU \n'%job)
    hostname = readwhile(pqsub, lambda line: line.startswith('HOSTNAME')).split('=')[1]
    readwhile(pqsub, lambda line: '[NotebookApp]' in line)
    
else:

    hostname, port = reconnect.split(',')
    
    
dotunnel(hostname,port)
    
print "Succesfully opened notebook!"
print "Kill this process to end your notebook connection."
print "To reconnect to this notebook later run:"
print "ipyn.carv --reconnect %s"%(','.join([hostname,port]))
print "To kill this notebook server run:"
print "ipyn.carv --kill %s"%job.split('.')[0]
time.sleep(12*60**2)

pqsub.kill()
ptunnel.kill()

print "Succesfully cleanup up connections."

