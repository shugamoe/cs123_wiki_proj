#!/bin/bash 

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

