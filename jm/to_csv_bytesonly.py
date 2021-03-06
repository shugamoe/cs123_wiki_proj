# Julian McClellan
# 5/20/16
#
# CS 123 Wikpedia Big Data Project
#
# Python file to convert mrjob output to CSV file that R can open and begin
# constructing linear models.
#
# Necessary because having mrjob output to CSV in the manner we want is hard.
# 
# This specific version is built to manually calculate bytes ratios, it turns
# out writing temporary files to store information when running an mrjob on s3 
# is a pain.  It would be nice to figure out, but this version of the function
# can calculate bytes ratios for us without having to get into the s3 hassle.
#
# Deprecated in favor of batch_to_csv.py.

from pathlib import Path
import pandas as pd
import os
import json

DIVIDER = '   '


def convert_output(files_path = os.getcwd(), name = '{}_{}-{}', test = 
    False):
    '''
    Reads the contents of mrjob output files into a dictionary, which is then
    turned into a pandas data frame (we utilize some useful built in indexing
    and sorting functions), and then lastly we write this data frame to a CSV.

    Inputs:
        <str> files_path: The path of the directory containing the output files
                          we would like to convert.
        <str> name: The name one would like to give to the file.
    '''
    homedir = os.getcwd()
    # Change the current working directory to the path of the files we want to
    # convert to a CSV.
    os.chdir(files_path) 

    # The main page we are interested in is always the name of the directory
    # containing the files.
    mpage = os.path.relpath(".", "..")

    # Begin creation of our dictionary which we will later convert to a CSV.
    # The keys correspond to the name of our columns in the eventual dataframe,
    # and the value is a list of tuples in which the first entry of the tuple
    # corresponds to the date (the row/index in the dataframe) and the second
    # value corresponds to the value one would see at the given row/index and
    # column in the dataframe.
    csv_dict = {}

    # Get filepaths of all mrjob mrjob_outputs
    mrjob_outputs = [pth.as_posix() for pth in Path.cwd().iterdir() if 'part-' 
    in pth.stem]

    # Parse each file.
    for part in mrjob_outputs:
        with open(part) as f:
            for line in f:
                entry = line.strip().strip('"').split(DIVIDER)
                page, date, views, bytes = entry[0], entry[1], entry[2], \
                entry[3]

                # If we happen upon our main page (whose traffic is the 
                # response variable in our regression) it will have an 
                # individual bytes entry which we will extract and track.
                #
                # We will use special formatting to indicate in our dictionary
                # that certain data pertains to our main page.
                if page == mpage:
                    csv_dict.setdefault('<bytes_{}>'.format(page), []).append(
                        (date, bytes))
                    csv_dict.setdefault('<traf_{}>'.format(page), []).append(
                        (date, views))
                else:
                    csv_dict.setdefault('traf_{}'.format(page), []).append(
                        (date, views))
                    # We will calculate the bytes ratio later later.
                    csv_dict.setdefault('bratio_{}'.format(page), []).append(
                        (date, bytes))

    # Enable manual inspection of dictionary prior to pandas friendly 
    # conversion.                
    if test:
        with open('test.json', 'w') as f:
            json.dump(csv_dict, f)

    # Extract the bytes values from the page of interest so we can calculate
    # bytes ratios.  
    pdates, pbytes = zip(*csv_dict['<bytes_{}>'.format(mpage)])

    # The dates associated with our page of interest are our master dates 
    # (master indices) for the data frame. 
    master_dates = list(pdates)
    master_dates.sort()

    # Convert dictionary to something pandas can easily make a dataframe with.
    for col_name, tuples in csv_dict.items():
        dates, values = zip(*tuples)
        dates = list(dates)
        values = list(values)

        # Code to detect, report and handle duplicate dates.
        dupes = set([x for x in dates if dates.count(x) > 1])
        if len(dupes) != 0:
            print('The following dates contain multiple entries:\t{}\tdata:'
                '{}'.format(dupes, col_name))
            for date in dupes:
                while dates.count(date) > 1:
                    ind = dates.index(date)
                    del dates[ind]
                    del values[ind]

        # If we find a bytes ratio column, we have to calculate the ratios.
        if 'bratio' in col_name:
            for index, bytes in enumerate(values):
                values[index] = bytes / pbytes[pdates.index(dates[index])]

        csv_dict[col_name] = pd.Series(list(values), index = list(dates))

    # Create the dataframe.  The we set the index parameter to master_dates,
    # the sorted list of all the dates appearing in the files.  
    # This ensures that even though information about column values might have
    # been received out of order, it will appear in order within the dataframe.
    try:
        csv_df = pd.DataFrame(csv_dict, index = master_dates)
        # While for the main page we are guaranteed to have information for any
        # date in the index, for the inlinks it is possible to have missing
        # data, so we dates where we are missing inlink data.
        csv_df.dropna(inplace = True)
    except ValueError:
        os.chdir(homedir) # So I can more easily rerun the file after an error
        raise


    dstart_str = master_dates[0].replace('/', '')
    dend_str = master_dates[-1].replace('/', '')

    # Write the CSV
    csv_df.to_csv((name + '.csv').format(mpage, dstart_str, dend_str), sep='\t')

    # Change back to current directory (makes shell testing easier).
    os.chdir(homedir) 