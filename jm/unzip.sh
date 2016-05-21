#!/bin/bash 
# This bash script, when inserted into a backup volume in which compressed 
# Wikipedia pagecounts info, will check what decompressed versions of the files
# are in our S3 Bucket (mounted to the instance via s3fs).  If our S3 Bucket
# is missing decompressed versions of the files, we will replace it with 
# a freshly decompressed files from the backup volume.

for f in *.gz; do
    STEM=$(basename "${f}" .gz)
    if [ -e ~/s3_wiki/pagecounts/"${STEM}" ]; then
        echo "${STEM}" already exists, skipping file.
    else 
        echo "${STEM}" not yet decompressed... Decompressing.
        sudo gunzip -c "${f}" > ~/s3_wiki/pagecounts/"${STEM}"
        sed -i '/^en /!d' ~/s3_wiki/pagecounts/"${STEM}"
    fi
done



