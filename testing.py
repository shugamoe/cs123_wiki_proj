import json
from pathlib import Path
import os


def unpack_json(json_sample_path):
    '''
    Function to unpack a json file containing the pages and inlinks to those
    pages we are interested in.
    '''
    with open(json_sample_path, 'r') as f:
        inlinks_dict = json.load(f)

    return inlinks_dict


def scrub_parse_output(files_path, json_path, num_inlinks = None):
    '''
    Function to update file to get the appropriate MRJob data for the 
    regression analysis

    This updates the MRJob_Step2 regression output to only include 
    homepage and datetimes that are in the data, and also the the 
    information of the appropriate links

    Inputs:
        <str> files_path: A string indicating the path of the directory 
                          containing the mrjob output
        <str> json_path: A string indicating the path of the json file that 
                         contains a dictionary whose keys correspond to 
                         pages of interest and values are a list of inlinks to 
                         these pages.  All the pages should have the same 
                         number of inlinks.
        <int> num_inlinks: The number of inlinks desired for each page.
                           If we do not see the desired number of inlinks
                           in the data for a page we will not make a CSV out of
                           it (incomplete data).  
    Outputs:
        <dict> new_inlinks_dict: A dictionary of all the pages and their 
                                 inlinks that appeared in the mrjob output.
    '''
    homedir = os.getcwd()
    os.chdir(files_path)

    # Call function to get all homepages and links of interest
    inlinks_dict = unpack_json(json_path)
    pages_of_int = list(inlinks_dict.keys())

    mrjob_outputs = [pth.as_posix() for pth in Path.cwd().iterdir() if 
    (pth.stem.startswith('part-') and not pth.stem.endswith('-conv'))]

    new_inlinks_dict = {}

    for output_file in mrjob_outputs:
        # Create a list of all infromation initial MRJob_Step2 output
        inlinks_mrjob = []
        with open(output_file, 'r') as f:
            for line in f:
                inlinks_mrjob.append(line.strip()[1:-1])

        # Create a dictionary of pages and datetimes in MRJob_Step2 data
        page_dates = {}
        # Create a list of relevant output for the MRJob_Step2 file
        final_lines = []
        
        for line in inlinks_mrjob:
            # fields = [pagename, datetime, pageviews, bytes]
            page_seen, date, views, bytes = line.split('   ')
            if page_seen in pages_of_int:
                page_dates[page_seen] = page_dates.get(page_seen, []) + [date]
                new_inlinks_dict[page_seen] = new_inlinks_dict.get(page_seen, 
                    [])
                # Include relevant links of the homepages
                final_lines.append(line)

        # print(list(keys.keys()))

        for line in inlinks_mrjob:
            # fields = [pagename, datetime, pageviews, bytes]
            page_seen, date, views, bytes = line.split('   ')

            # Handle inlinks.
            if page_seen not in page_dates.keys():
                for key in page_dates.keys():
                    if page_seen in inlinks_dict[key]:
                        if date in page_dates[key]:
                            final_lines.append(line)
                            if page_seen not in new_inlinks_dict[key]:
                                new_inlinks_dict[key] = new_inlinks_dict[key] \
                                + [page_seen]
                            break

        # Overwrite the initial MRJob_Step2 file
        with open(output_file + '-conv', 'w') as f:
            for line in final_lines: 
                f.write(line + "\n")

    # Return to starting directory
    os.chdir(homedir)


    # Make sure everything in our new_inlinks_dict has the expected number of 
    # inlinks
    if num_inlinks:
        kill_list = []
        for page in new_inlinks_dict:
            if len(new_inlinks_dict[page]) != num_inlinks:
                kill_list.append(page)

        for page in kill_list:
            del new_inlinks_dict[page]

    return new_inlinks_dict
