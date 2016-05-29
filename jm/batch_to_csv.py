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
# This specific version is built to manually calculate bytes ratios, but also
# to have the ability to create multiple csv files from a single batch of mrjob
# output that contains information for more than one regression (each 
# regression depends on a single csv file).

from pathlib import Path
import pandas as pd
import os
import json
from inlinks import two_inlinks_sample
import sys
from testing import scrub_parse_output

DIVIDER = '   '


def run(link_json_path, mrjob_output_path, num_inlinks, merged = False, 
    test = False):
    '''
    Wrapper function for make_all_csvs.  We first call a function that scrubs 
    the mrjob data and parses the output.  It returns a dictionary that alerts
    us as to what complete page information the scrubbed output will have.

    Then, we pass that dictionary, along with the path of the scrubbed files 
    (which should be the same as the original path) to our make_all_csvs 
    function to make either one or several CSV files.

    Inputs:
        <str> link_json_path: A string indicating the path of the json file
                              that contains a master dictionary containing
                              information on pages with a certain number of
                              links.
        <str> mrjob_output_path: A string indicating the path of the directory
                                 containing our mrjob output.
        <int> num_inlinks: This is the number of inlinks we expect each page to
                           have.  This gives us the option to utilize the 
                           merged boolean to stack our dataset.  
        <bool> merged: A boolean indicating whether or not the user would
                       like to create only a merged CSV file that combines all
                       the results of what would have been the individual CSV
                       files.
        <bool> test: A boolean  indicating whether or not the user would like
                     to output intermediate JSON files to manually inspect 
                     some results.
    Outputs:
        None: Writes one or several CSV files.
    '''
    links_dict = scrub_parse_output(mrjob_output_path, link_json_path, 
        num_inlinks)
    make_all_csvs(links_dict, mrjob_output_path, num_inlinks, merged, test)


def make_all_csvs(links_dict, files_path = os.getcwd(), num_inlinks, 
    merged = False, test = False):
    '''
    Reads the contents of mrjob output files into dictionaries, which are then
    turned into pandas data frames (we utilizec some useful built in indexing
    and sorting functions), and then lastly we write these data frames to 
    multiple or a single (assuming each page in the links_dict contains the
    same number of pages).  

    Inputs:
        <dict> links_dict: This is a special dictionary whose keys correspond 
                           to the pages we are interested in testing, and whose
                           values correspond to the inlinks to the given pages.
        <str> files_path: The path of the directory containing the output files
                          we would like to convert.
        <bool> merged: A boolean indicating whether or not the user would
                       like to create only a merged CSV file that combines all
                       the results of what would have been the individual CSV
                       files.
        <bool> test: A boolean  indicating whether or not the user would like
                     to output intermediate JSON files to manually inspect 
                     some results.
    Outputs:
        None: Either writes one or several CSV files. 

    example links_dict:

    {'America': ['Bible Belt', 'Illuminati'],
     'Andy Zhu': ['Asian', 'Totally Yolked']}

    If this above links_dict was provided, with merged = False,  this function
    would create two csvs, one concerning the page 'America' and it's inlinks 
    'Bible Belt' and 'Illuminati', and another concerning the page 'Andy Zhu'
    and the inlinks to that page 'Asian', and 'Totally Yolked'.
    ''' 
    print("Looking for: \n{}\n".format(links_dict))
    homedir = os.getcwd()
    # Change the current working directory to the path of the files we want to
    # convert to a CSV.
    os.chdir(files_path) 

    csv_dicts = {}
    num_pages = 0
    for mpage in links_dict:
        csv_dicts[mpage] = {}
        num_pages += 1

    # Get filepaths of all mrjob mrjob_outputs
    mrjob_outputs = [pth.as_posix() for pth in Path.cwd().iterdir() if 
    pth.stem.endswith('-conv')]

    # Parse each file, adding the information contained within each entry to 
    # the appropriate dictionary within csv_dicts.
    for part in mrjob_outputs:
        with open(part, 'r') as f:
            for line in f:
                page, date, views, bytes = line.strip().split(DIVIDER)

                match_found = False
                for mpage, inlinks in links_dict.items():
                    if page == mpage:
                        match_found = True
                        csv_dicts[mpage].setdefault('<bytes_{}>'.format(page), 
                            []).append((date, bytes))
                        csv_dicts[mpage].setdefault('<traf_{}>'.format(page), 
                            []).append((date, views))
                    elif page in inlinks:
                        match_found = True
                        csv_dicts[mpage].setdefault('traf_{}'.format(page), []
                            ).append((date, views))
                        # We will calculate the bytes ratios later.
                        csv_dicts[mpage].setdefault('bratio_{}'.format(page), 
                            []).append((date, bytes))
                if not match_found:
                    # print("The following entry did not correspond to any known"
                    #     "page of interest or inlinks to these pages\n", line)
                    pass

    # Enable manual inspection of dictionary prior to pandas friendly 
    # conversion.                
    if test:
        with open('rawdict.json', 'w') as f:
            json.dump(csv_dicts, f)
    
    if merged:
        cols = ['<bytes_page>', '<traf_page>'] + ['bratio_il{}'.format(count) 
                for count in range(num_inlinks)] + ['traf_il{}'.format(count) 
                for count in range(num_inlinks)]
        data_frames = []
        # Our initial min date is actually the max (so any real minimum date 
        #  will be smaller than this).  Similar logic applies for our initial
        # max date.
        comb_min_date = '2010020623'
        comb_max_date = '2008100100'

    for mpage in csv_dicts:
         loc_min_date, loc_max_date, df = dict_to_df(mpage, csv_dicts[mpage], 
            homedir, merged, test)
         if merged:
            comb_min_date = min(loc_min_date, comb_min_date)
            comb_max_date = max(loc_max_date, comb_max_date)
            data_frames.append(df)
         else:
            df.to_csv('{}_{}-{}.csv'.format(mpage, loc_min_date, loc_max_date))

    if merged:
        merged_df = pd.concat(data_frames, ignore_index = True, keys = cols)
        merged_df.to_csv('{}_pages_{}-{}.csv'.format(num_pages, comb_min_date, 
            comb_max_date))

    # Change back to current directory when finished 
    # (makes shell testing easier).
    os.chdir(homedir)


def dict_to_df(mpage, info_dict, homedir, test = False):
    '''
    Creates a df that containing all relevant information about mpage, the 
    current page of interest.

    Inputs:
        <str> mpage: The name of our page of interest, whose traffic is the 
                     response variable in our regression.
        <dict> info_dict: A python dictionary containing all of the relevant
                          information for the mpage and its inlinks from the
                          mrjob output.
        <str> homedir: String that contains the filepath of this Python file
                       if we encounter issues we revert to this as our working
                       directory.
        <bool> test: A boolean  indicating whether or not the user would like
                     to output intermediate JSON files to manually inspect 
                     some results.

    Outputs:
        <str> loc_min_date: A string indicating the lowest valued date seen in
                            the info_dict.
        <str> loc_max_date: A string indicating the highest valued date seen in
                            the info_dict.
        <pd df> df: A pandas dataframe created from the info_dict.  
    '''
    master_dates = convert_dict(mpage, info_dict, test)

    # Create the dataframe.  The we set the index parameter to master_dates,
    # the sorted list of all the dates appearing in the files.  
    # This ensures that even though information about column values might have
    # been received out of order, it will appear in order within the dataframe.
    try:
        df = pd.DataFrame(info_dict, index = master_dates)
        # While for the main page we are guaranteed to have information for any
        # date in the index, for the inlinks it is possible to have missing
        # data, so we dates where we are missing inlink data.
        df.dropna(inplace = True)
    except ValueError:
        os.chdir(homedir) # So I can more easily rerun the file after an error
        raise

    loc_min_date = master_dates[0].replace('/', '')
    loc_max_date = master_dates[-1].replace('/', '')

    return loc_min_date, loc_max_date, df


def convert_dict(mpage, info_dict):
    '''
    This function takes the raw dictionary created from parsing the mrjob 
    output and converts it the values in the dictionary to a pandas series.
    Before the data is convereted to pandas series we check for duplicate data
    and calculate the bytes ratios.    

    Inputs:
        <str> mpage: The name of our page of interest, whose traffic is the 
                     response variable in our regression.
        <dict> info_dict: A dictionary containing the raw information skimmed
                          from the mrjob output.
    '''
    # Extract the bytes values from the page of interest so we can calculate
    # bytes ratios.  
    pdates, pbytes = zip(*info_dict['<bytes_{}>'.format(mpage)])

    # The dates associated with our page of interest are our master dates 
    # (master indices) for the data frame. 
    master_dates = list(pdates)
    master_dates.sort()

     # Convert dictionary to something pandas can easily make a dataframe with.
    for col_name, tuples in info_dict.items():
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
                values[index] = int(bytes) / int(pbytes[pdates.index(
                    dates[index])])

        # Replace the value in the dictionary with a pandas Series to enable
        # easy creation of a dataframe later.
        info_dict[col_name] = pd.Series(list(values), index = list(dates))

    return master_dates
    

