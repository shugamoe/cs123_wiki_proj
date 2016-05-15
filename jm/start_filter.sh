#!/bin/bash 
./mount_s3.sh
mv enonly.sh s3_wiki/pagecounts
cd s3_wiki/pagecounts

if [ $1 = "r" ]
    ./enonly.sh r
else
    ./enonly.sh





