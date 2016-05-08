#!/bin/bash 

# Take down some commands that might be useful for automated setup of EC2
# instances and attaching volumes so that we can start MRjobs more quickly.

# Create those micro instances that we've been working with.
#
# What is returned is a JSON format of all the information contained in the 
# newly formed instance.  I would like to extract the newly created instance
# ID so that I can automate attaching volumes to it with the aws ec2 
# attach-volume command.  
#
# However, I cannot understand how to conveniently parse the JSON output
INST_INFO=$(aws ec2 run-instances --image-id ami-f5f41398 --count 1 \
    --instance-type t2.micro --key-name cslab)

# The instance ID is something like "i-stuff", could possibly extract it by
# finding something that started with an "i and ended with a ".
INST_ID=<Insert command that could extract the instance id>


# Attach a volume to an instance
aws ec2 attach-volume --volume-id <vol-1234abcd> \
--instance-id <i-01474ef662b89480> --device /dev/sdf