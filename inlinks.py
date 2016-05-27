import pandas as pd
# import numpy as np 
# import matplotlib.pyplot as plt
import json


def wiki_homepages(pagename, json_file, titles_file):
    '''
    Generates a list of inlinks (pages that link to pagename) of a page.
    Inputs:
        pagename - name of the page of interest
        json_file - file of the json containing the line numbers of 
                    pages and their inlinks
        titles_file - file containing pagenames of each number in json_file
    Output:
        list of inlinks
    '''

    # Generates a dataframe containing pagenames for each line number
    titles = pd.read_csv(titles_file, delimiter = ' ', names = ['page title'])
    # Finds the line index of pagename
    line_index = titles[titles[[0]] == pagename].dropna().index.tolist() 
    
    if line_index == []: 
        print('pagename does not exist')
        return None

    page_line_num = line_index[0] + 1

    # Opens json_file, extracts the list of inlinks of pagename, and closes 
    # json_file
    f = open(json_file, 'r')
    homepage_line_nums = json.load(f).get(str(page_line_num), [])
    f.close()
    homepage_titles = []
    for homepage_line_num in homepage_line_nums:
        title = titles.iloc[[int(homepage_line_num) - 1]].values[0][0]
        # appends the title of each inlink to a list
        homepage_titles.append(title) 

    return homepage_titles


def one_to_five_inlinks_dump(json_file, titles_file):
    titles = pd.read_csv(titles_file, delimiter = ' ', names = ['page title'])

    one_to_five_dict = {}
    with open(json_file, 'r') as f:
        count = 1
        for key, val in json.load(f).items():
            if len(val) == 2:
                key_name = titles.iloc[[int(key) - 1]].values[0][0]
                for v in val:
                    val_name = titles.iloc[[int(v) - 1]].values[0][0]
                    one_to_five_dict[key_name] = one_to_five_dict.get(key_name, 
                        []) + [val_name]
            count += 1
            if count % 1000 == 0:
                print(count)

    with open('one_to_five_inlinks', 'w') as f:
        json.dump(one_to_five_dict, f)


def two_inlinks_sample(json_file_one_to_five):
    
    two_inlinks_sample = {}

    with open(json_file_one_to_five, 'r') as f:
        inlinks_dict = json.load(f)

    two_inlinks_sample['Wrestling_Slang'] = inlinks_dict['Wrestling_Slang']
    two_inlinks_sample['Concordia_University,_St._Paul'] = \
        inlinks_dict['Concordia_University,_St._Paul']
    two_inlinks_sample['A_Spaceman_Came_Travelling_(Christmas_Remix)'] = \
        inlinks_dict['A_Spaceman_Came_Travelling_(Christmas_Remix)']
    two_inlinks_sample['Transcendentals'] = inlinks_dict['Transcendentals']
    two_inlinks_sample['Platinum_Card'] = inlinks_dict['Platinum_Card']

    return two_inlinks_sample








