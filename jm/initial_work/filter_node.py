# Julian McClellan
#
# CS 123 Wikpedia Big Data Project
#
# Python Script to create nodes that will filter out lines in our traffic 
# data not beginning with "en" over a certain date range.
#
# This function is the building block for an ad-hoc cluster of sorts.  
# Ended up filtering the data from 2.5TB to 660GB by running this function in
# 20 different terminal windows.  

import sys 
import subprocess
import re
import json
import time


def launch_filter_node(inst_type):
    '''
    This function creates a node that will filter out a certain date range of 
    our data such that the files filtered will only contain lines pertaining
    to proper language wikipedia pages.

    The function launches the instance, uploads the necessary bash scripts to
    the instance, and then connects to the instance via ssh. 

    Inputs:
        <str> inst_type: The type of AWS instance we want to utilize.
    Outputs:
        None
    '''
    # Launch EC2 Instance
    launch_com = "aws ec2 run-instances --image-id ami-f5f41398 --count 1 \
    --instance-type {} --key-name cslab --security-group-ids sg-b0f73fcb".format(inst_type)
    launch_proc = subprocess.Popen(launch_com, stdout = subprocess.PIPE, 
        shell = True)

    # Extract instance id <i-stuff> from bash stdout.
    prelim_info = json.loads((launch_proc.stdout.read()).decode("utf-8"))
    inst_id = prelim_info['Instances'][0]['InstanceId']
    print("Launched {} EC2 Instance with ID: {}\n\n".format(inst_type, inst_id))

    print("Waiting 120 seconds (instance spooling up).\n")
    time.sleep(120) # Wait for instance to to spool up
    print("Sleeping Completed, will now retrieve public DNS\n\n")

    # Get the public DNS from the instance information
    dns_com = "aws ec2 describe-instances --instance-ids {} --output json".format(
        inst_id)
    dns_proc = subprocess.Popen(dns_com, stdout = subprocess.PIPE, shell = True)

    inst_info = json.loads((dns_proc.stdout.read()).decode("utf-8"))

    dns = inst_info['Reservations'][0]['Instances'][0]['PublicDnsName']
    print("[FUll] Public DNS is: ec2-user@{}\n\n".format(dns))

    # Let's upload our bash necessary bash scripts

    # Script that mounts our S3 Bucket
    s3up_com = "scp -o StrictHostKeyChecking=no -i ~/cslab.pem ~/cs123_wiki_proj/jm/mount_s3.sh ec2-user@{}:~/".format(dns)
    subprocess.Popen(s3up_com, shell = True)
    print("mount_s3.sh uploaded\n")

    # Script that filters the actual files
    enup_com = "scp -o StrictHostKeyChecking=no -i ~/cslab.pem ~/cs123_wiki_proj/jm/enonly.sh ec2-user@{}:~/".format(dns)
    subprocess.Popen(enup_com, shell = True)
    print("enonly.sh uploaded\n")

    # Command script that runs and/or puts these scripts into the proper place
    sfup_com = "scp -o StrictHostKeyChecking=no -i ~/cslab.pem ~/cs123_wiki_proj/jm/start_filter.sh ec2-user@{}:~/".format(dns)
    subprocess.Popen(sfup_com, shell = True)
    print("start_filter.sh uploaded\n\n")

    # Give the ssh command to the user
    print("Use the following command to connect to instance:\n\n")
    print("ssh -i ~/cslab.pem ec2-user@{}".format(dns))


if __name__ == "__main__":
    inst_type = sys.argv[1]
    launch_filter_node(inst_type)