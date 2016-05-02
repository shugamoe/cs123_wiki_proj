#!/bin/bash 

# Bash script for finding a range file dates for Wikipedia traffic data


read -p "Enter a starting date: YYYYMMDD" START
read -p "Enter an ending date: YYYYMMDD" END
read

mkdir ~/$START'_'$END
export MATCHINGMATCHES=`echo *{$START..$END}*`



BUSH="./speeches/bush1+2.txt"
KERRY="./speeches/kerry1+2.txt"
OBAMA="./speeches/obama1+2.txt"
MCCAIN="./speeches/mccain1+2.txt"

for maybe_gz in MATCHES; do
    python3 ./Markov.py $BUSH $KERRY $quote $K 
    if [[ $maybe_gz == *gz* ]];
    then
        echo cp $maybe_gz ~/
    else
        echo caught $maybe_gz, was not a gz file.
    fi

    if [[ $quote == ./speeches/bush-kerry3/KERRY* ]];
    then
        echo "  Answer:   Speaker B"
    fi
    echo
done

# This command finds all the files in the folder with years from 2009 to 2010,
# from Jan to Feb, from the 1st and 2nd days of feb, and from hours 1 through 
# 12
echo *{2009..2010}{01..02}{01..02}-{01..12}*

