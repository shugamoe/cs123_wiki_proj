#!/bin/bash 


read -p "Enter hour from 00 to 23 <HH>:" HOURS

for f in *-$HOURS00*; do
    echo Filtering $f
    sed -i '/^en /!d' $f
done



