#!/bin/bash 


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
