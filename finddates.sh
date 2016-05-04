#!/bin/bash 

# Bash script for finding a range file dates for Wikipedia traffic data
# Execute the bash script in the ./wikistats/pagecounts folder.


read -p "Enter a starting year: (2008-2010)" YEAR_S
read -p "Enter an ending year: (2008-2010)" YEAR_E
read -p "Enter a starting month: (01-12)" MONTH_S
read -p "Enter an ending month: (01-12)" MONTH_E
read -p "Enter a starting day: (01-31)" DAY_S
read -p "Enter an ending day: (01-31)" DAY_E
read -p "Enter a starting hour (00-23)" HOUR_S
read -p "Enter an ending hour (00-23)" HOUR_E

export DEST=Y$YEAR_S'-'$YEAR_E'_'M$MONTH_S'-'$MONTH_E'_'D$DAY_S'-'DAY_E'_'H$HOUR_S'_'$HOUR_E

# For now, throw copied files to a labeled folder in the home directory. If this 
# bash script is used for large date ranges, the memory on the AWS instance
# might not be big enough to hold.  Might have to first create a large enough
# volume, attach it, and then make that place the destination.

mkdir ~/mnt/processed/$DEST
export MATCHES=`echo *{$YEAR_S..$YEAR_E}{$MONTH_S..$MONTH_E}{$DAY_S..$DAY_E}-{$DAY_S..$DAY_E}*`


for maybe_gz in MATCHES; do
    if [[ $maybe_gz == *gz* ]];
    then
        echo cp $maybe_gz ~/mnt/processed/$DEST
    else
        echo caught $maybe_gz, was not a gz file.
    fi
done

chmod -r 777 ~/mnt/processed/$DEST # This allows the files to then be unzipped.
cd ~/mnt/processed/$DEST
find . -type f -exec sed -n '/^#/!p' {} \;




# Could add some unzipping stuff in here

# This command finds all the files in the folder with years from 2009 to 2010,
# from Jan to Feb, from the 1st and 2nd days of feb, and from hours 1 through 
# 12
# echo *{2009..2010}{01..02}{01..02}-{01..12}*

