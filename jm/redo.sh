#!/bin/bash
# During the removal of non-English entries in the data, it came to my attention
# that some filtered files had a size of 0 bytes afterword.
#
# I made this bash script to remove the offending files and have unzip.sh 
# put a fresh copy in so I could do some quick experiments.
#
# Turns out, (obviously in hindsight) that some Wikipedia traffic files do not
# contain any English entries whatsoever, which makese sense, assuming the 
# files contain only a sample of all Wikipedia traffic.
 

cd ~/s3_wiki/pagecounts/
echo Finding Files...
for f in `find . -type f -size -30M`; do
    if [[ $f == *"pagecounts"* ]]; then
        echo Found suspiciously small file $f, removing...
        rm $f
        echo Removed $f from S3 Bucket.
    fi
done
cd
echo Replacing Files...
./unzip.sh

