# Python Script to create nodes that will filter out lines in our traffic 
# data not beginning with "en" over a certain date range.

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

    print("Waiting 40 seconds (instance spooling up).\n")
    time.sleep(120) # Wait for instance to to spool up
    print("Sleeping Completed, will now retrieve public DNS\n\n")

    # Get the public DNS from the instance information
    dns_com = "aws ec2 describe-instances --instance-ids {} --output json".format(
        inst_id)
    dns_proc = subprocess.Popen(dns_com, stdout = subprocess.PIPE, shell = True)

    inst_info = json.loads((dns_proc.stdout.read()).decode("utf-8"))

    dns = inst_info['Reservations'][0]['Instances'][0]['PublicDnsName']
    print("Public DNS is: ec2-user@{}\n\n".format(dns))

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


    # Wait until the server is fully initialized for an ssh connection.

    # while True:
    #     print("Waiting 5 Seconds before checking if instance ready")
    #     time.sleep(5) 
    #     check_com = "aws ec2 describe-instances --instance-ids {} --output table".format(inst_id)
    #     check_proc = subprocess.Popen(launch_com, stdout = subprocess.PIPE, 
    #     shell = True)
    #     info = json.loads((check_proc.stdout.read()).decode("utf-8"))


    # Give the ssh command to the user
    # print("Use the following command to connect to instance:\n")
    # print("ssh -i ~/cslab.pem ec2-user@{}".format(dns))


if __name__ == "__main__":
    inst_type = sys.argv[1]
    launch_filter_node(inst_type)