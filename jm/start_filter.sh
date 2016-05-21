#!/bin/bash 
# This is a script that when run, mounts our S3 bucket onto the instance
# and then enables the removal of non-english entries in the data by 
# utilizing the enonly.sh script.

./mount_s3.sh
mv enonly.sh s3_wiki/pagecounts
cd s3_wiki/pagecounts

if [ $1 = "r" ]
    ./enonly.sh r
else
    ./enonly.sh
fi





