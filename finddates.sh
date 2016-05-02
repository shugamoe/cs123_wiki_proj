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
    fi

    if [[ $quote == ./speeches/bush-kerry3/KERRY* ]];
    then
        echo "  Answer:   Speaker B"
    fi
    echo
done

echo
echo
echo

for quote in ./speeches/obama-mccain3/*.txt; do
    echo $quote
    python3 ./Markov.py $OBAMA $MCCAIN $quote $K
    if [[ $quote == ./speeches/obama-mccain3/OBAMA* ]];
    then
        echo "  Answer:   Speaker A"
    fi

    if [[ $quote == ./speeches/obama-mccain3/MCCAIN* ]];
    then
        echo "  Answer:   Speaker B"
    fi
    echo
done


string='My long string';

if [[ $string == *"My long"* ]]
then
  echo "It's there!";
fi