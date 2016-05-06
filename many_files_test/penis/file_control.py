# Python Script to start automated setup of EC2
# instances and attaching volumes so that we can start MRjobs more quickly.

# echo "AKIAJ7KNXNMC4B2N3JIA:ejJ3DjBQ9dniU5m/UlpvcY+SSZO3VD9BZqJzpuTI" > .passwd-s3fs

import sys 
import subprocess
import re

EN_ONLY_VOL = "vol-1234abcd" 




def move_files():
    '''
    This function launches a t2.micro workspace and attaches our final volume
    to it.
    '''
    get_files = "echo *{2009..2010}{01..12}{01..31}*"

    # Launch EC2 Instance
    launch_proc = subprocess.Popen("ls", shell = True, stdout = subprocess.PIPE)

    files = launch_proc.stdout.read()
    files = files.decode("utf-8") 
    files = files.split('\n')
    files.pop(-1) # last one is a blank space.


    for gz in files:
        copy_command = "cp {} penis".format(gz)
        subprocess.Popen(copy_command, shell = True, stdout = subprocess.PIPE)

    print("Done!")
