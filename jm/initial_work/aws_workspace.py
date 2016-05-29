# Julian McClellan
#
# CS 123 Wikpedia Big Data Project

import sys 
import subprocess
import re

VOLUME = "vol-1234abcd" # This volume contains writeable versions of our data.

def launch_ec2_workspace():
    '''
    This function launches a t2.micro workspace and attaches a volume containing
    writeable versions of our data to that instance.  
    '''
    launch_command = "aws ec2 run-instances --image-id ami-f5f41398 --count 1 \
    --instance-type t2.micro --key-name cslab"

    # Launch EC2 Instance
    launch_proc = subprocess.Popen(launch_command, stdout = subprocess.PIPE)
    id_pat = r'\"(i-.*)\"'

    # Extract instance id <i-stuff> from bash stdout.
    inst_id = re.findall(id_pat, launch_proc.stdout.read())

    # Attach our volume of English only data to our instance.
    attach_vol_proc = "aws ec2 attach-volume --volume-id {} --instance-id {} \
    --device /dev/sdf".format(inst_id, VOLUME)


if __name__ == "__main__":
    launch_ec2_workspace()
