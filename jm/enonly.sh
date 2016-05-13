#!/bin/bash 


read -p "Enter a date range:" DATES

for f in pagecounts-$DATES*; do
    echo Filtering $f
    sed -i '/^en[[:space:]]/!d' $f
done



