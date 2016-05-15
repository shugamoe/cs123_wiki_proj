#!/bin/bash 

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



