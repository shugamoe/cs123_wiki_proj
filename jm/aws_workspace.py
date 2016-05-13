# Python Script to start automated setup of EC2
# instances and attaching volumes so that we can start MRjobs more quickly.

import sys 
import subprocess
import re

EN_ONLY_VOL = "vol-1234abcd" 



def launch_ec2_workspace():
    '''
    This function launches a t2.micro workspace and attaches our final volume
    to it.
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
    --device /dev/sdf".format(inst_id, EN_ONLY_VOL)


if __name__ == "__main__":
    launch_ec2_workspace()






scp -i ~/cslab.pem mount_s3.sh ec2-user@$ADDR:~/
ssh -i ~/cslab.pem ec2-user@$ADDR
./mount_s3.sh
exit
scp -i ~/cslab.pem enonly.sh ec2-user@$ADDR:~/s3_wiki/pagecounts/
ssh -i ~/cslab.pem ec2-user@$ADDR