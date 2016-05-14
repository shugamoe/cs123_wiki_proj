#!/bin/bash 


read -p "Enter hour from 000 to 230 <HH0>:" HOURS

for f in *-$HOURS*; do
    echo Filtering $f
    sed -i '/^en /!d' $f
done



