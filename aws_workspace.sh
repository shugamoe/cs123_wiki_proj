#!/bin/bash 

# Take down some commands that might be useful for automated setup of EC2
# instances and attaching volumes so that we can start MRjobs more quickly.

# Create those micro instances that we've been working with
INST_INFO=$(aws ec2 run-instances --image-id ami-f5f41398 --count 1 --instance-type t2.micro --key-name cslab)

# Attach a volume to an instance
aws ec2 attach-volume --volume-id <vol-1234abcd> --instance-id <i-01474ef662b89480> --device /dev/sdf