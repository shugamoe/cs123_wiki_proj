#!/bin/bash 

for f in *.gz; do
    STEM=$(basename "${f}" .gz)
    if [ -e ~/wikitrafv2big/pagecounts/"${STEM}" ]; then
        echo "${STEM}" not yet decompressed... Decompressing.
        sudo gunzip -c "${f}" > ~/wikitrafv2big/pagecounts/"${STEM}"
    else 
        echo "${STEM}" already exists, skipping file.
    fi 



