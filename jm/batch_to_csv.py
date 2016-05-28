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


def make_all_csvs(links_dict, files_path = os.getcwd(), combined = False, 
    test = False):
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
        <bool> combined: A boolean indicating whether or not the user would
                         like to create only one combined CSV file.  If False
                         one CSV file will be created for each page and its 
                         inlinks.
        <bool> test: A boolean  indicating whether or not the user would like
                     to output intermediate JSON files to manually inspect 
                     some results.
    Outputs:
        None: Either writes one or several CSV files. 

    example links_dict:

    {'America': ['Bible Belt', 'Illuminati'],
     'Andy Zhu': ['Asian', 'Last name sounds like the word Jew']}

    If this above links_dict was provided, with combined = False,  this function
    would create to csvs, one concerning the page 'America' and it's inlinks 
    'Bible Belt' and 'Illuminati', and another concerning the page 'Andy Zhu'
    and the inlinks to that page 'Asian', and 'Last name sounds like the word 
    Jew'.

    If the example links_dict was given and combined = True, this function would
    create a single CSV, because in this case we would be interested in a 
    single CSV that contains the combined information of any number of pages
    that all have the same number of inlinks.
    ''' 
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
                    print("The following entry did not correspond to any known"
                        "page of interest or inlinks to these pages\n", line)
    print("Output Parsed, proceeding to convert dictionaries to dataframes")

    # Enable manual inspection of dictionary prior to pandas friendly 
    # conversion.                
    if test:
        with open('rawdict.json', 'w') as f:
            json.dump(csv_dicts, f)
    
    if combined:
        data_frames = []
        # Our initial min date is actually the max (so any real minimum date 
        #  will be smaller than this).  Similar logic applies for our initial
        # max date.
        comb_min_date = '2010020623'
        comb_max_date = '2008100100'

    for mpage in csv_dicts:
         loc_min_date, loc_max_date, df = dict_to_df(mpage, csv_dicts[mpage], 
            homedir, combined, test)
         if combined:
            comb_min_date = min(loc_min_date, comb_min_date)
            comb_max_date = max(loc_max_date, comb_max_date)
            data_frames.append(df)
         else:
            df.to_csv('{}_{}-{}.csv'.format(mpage, loc_min_date, loc_max_date))

    if combined:
        combined_df = pd.concat(data_frames)
        combined_df.to_csv('{}_pages_{}-{}.csv'.format(num_pages, comb_min_date, 
            comb_max_date))

    # Change back to current directory when finished 
    # (makes shell testing easier).
    os.chdir(homedir)


def dict_to_df(mpage, info_dict, homedir, combined = False, test = False):
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
        <bool> combined: A boolean indicating whether or not the user would
                         like to create only one combined CSV file, rather than
                         several.  In this function this argument is simply 
                         passed to the convert_dict function.
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
    master_dates = convert_dict(mpage, info_dict, combined, test)

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



def convert_dict(mpage, info_dict, combined = False, test = False):
    '''
    This function takes the raw dictionary created from parsing the mrjob 
    output and converts it the values in the dictionary to a pandas series.  
    If a combined CSV is desired, the keys of the dictionary will be converted
    to take on a uniform apperance such that the information of any two 
    converted dictionaries with information on pages containing the same number
    of inlinks can actually be merged together.  (Note, this merging is 
    actually done at the pandas dataframe level though, but the idea is the 
    same)

    Inputs:
        <str> mpage: The name of our page of interest, whose traffic is the 
                     response variable in our regression.
        <dict> info_dict: A dictionary containing the raw information skimmed
                          from the mrjob output.
        <bool> combined: A boolean indicating whether or not the user would
                         like to create only one combined CSV file.  If true,
                         this function will give the keys of the dictionary a
                         uniform appearance.  
        <bool> test: A boolean  indicating whether or not the user would like
                     to output intermediate JSON files to manually inspect 
                     some results.

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

    # If combined bool is true, we provide a general naming scheme that enables
    # us to combine multiple dataframes concerning pages with the same number
    # of inlinks.
    if combined:
        titles = []
        ignore = set()
        for col_name in info_dict:
            if col_name in ignore:
                pass
            elif mpage in col_name:
                if 'bytes' in col_name:
                    info_dict['<bytes_page>'] = info_dict.pop(col_name)
                    ignore.add('<bytes_page>')
                elif 'traf' in col_name:
                    info_dict['<traf_page>'] = info_dict.pop(col_name)
                    ignore.add('<traf_page>')
            else:
                if 'bratio' in col_name:
                    page_name = col_name.lstrip('bratio_')
                    if page_name not in titles:
                        titles.append(page_name)
                    title_id = titles.index(page_name)
                    new_col_name = 'bratio_il{}'.format(title_id)
                    info_dict[new_col_name] = info_dict.pop(col_name)
                    ignore.add(new_col_name)
                elif 'traf' in col_name:
                    page_name = col_name.lstrip('traf_')
                    if page_name not in titles:
                        titles.append(page_name)
                    title_id = titles.index(page_name)
                    new_col_name = 'traf_il{}'.format(title_id)
                    info_dict[new_col_name] = info_dict.pop(col_name)
                    ignore.add(new_col_name)

    return master_dates
    

def run(link_json_path, mrjob_output_path, num_inlinks = None, combined = False, 
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
                           have.  If specified, this ensures that the
                           dictionary returned will only have pages with the 
                           specified number of inlinks.
        <bool> combined: A boolean indicating whether or not the user would
                         like to create only one combined CSV file.  If False
                         one CSV file will be created for each page and its 
                         inlinks.
        <bool> test: A boolean  indicating whether or not the user would like
                     to output intermediate JSON files to manually inspect 
                     some results.
    Outputs:
        None: Writes one or several CSV files.
    '''
    links_dict = scrub_parse_output(mrjob_output_path, link_json_path, 
        num_inlinks)
    make_all_csvs(links_dict, mrjob_output_path, combined, test)