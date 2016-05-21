#!/bin/bash
# This bash script is utilized to filter non-English entries from our 
# uncompressed data files.  
#
# The user inputs an hour of data they wish to filter.  Originally this was 
# so that I could set up 24 instances each scrubbing one hour (1/24th) of the
# data, but AWS would only let me use 20 instances.
#
# See 'filter_node.py', 'mount.sh', or 'start_filter.sh' for more details.

read -p "Enter hour from 000 to 230 *-<HH0>[*]:" HOURS

if [ $1 = "r"]
    for f in `find -name "*-$HOURS" | tac`; do
        echo Filtering $f
        sed -i '/^en /!d' $f
    done
else
    for f in `find -name "*-$HOURS"`; do
        echo Filtering $f
        sed -i '/^en /!d' $f
    done
fi
