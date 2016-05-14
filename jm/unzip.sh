#!/bin/bash 

for f in *.gz; do
    STEM=$(basename "${f}" .gz)
    if [ -e ~/wikitrafv2big/pagecounts/"${STEM}" ]; then
        echo "${STEM}" already exists, skipping file.
    else 
        echo "${STEM}" not yet decompressed... Decompressing.
        sudo gunzip -c "${f}" > ~/wikitrafv2big/pagecounts/"${STEM}"
    fi
done



