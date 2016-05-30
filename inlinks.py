import pandas as pd
import json
import urllib.parse


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


def one_to_five_inlinks_dump(json_file):
    '''
    Generates a dict of pages with one to five inlinks (pages that link to 
    pagename) of a page, and dumps it to a json file named 'one_to_five_inlinks'.
    Inputs:
        json_file - file of the json containing the line numbers of 
                    pages and their inlinks
    '''

    one_to_five_dict = {}
    with open(json_file, 'r') as f:
        for key, val in json.load(f).items():
            # if the pagename contains one to five inlinks, append to dict
            if len(val) >= 1 and len(val) <= 5: 
                one_to_five_dict[key] = val


    with open('one_to_five_inlinks', 'w') as f:
        json.dump(one_to_five_dict, f)


def one_to_five_inlinks_sample_dump(json_file, titles_file, num_of_inlinks, \
    num_of_pages):
    '''
    Generates a dict of num_of_pages pages with num_of_inlinks inlinks by 
    loading json_file (a dict containing all pages with one to five inlinks), 
    filtering through, and appending suitable items into the dict inlinks_sample.
    inlinks_sample is then dumped into a sample directory, which is used for
    the function one_to_five_inlinks_sample.
    Inputs:
        json_file - file of the json containing the line numbers of 
                    pages and their inlinks
        titles_file - file containing pagenames of each number in json_file
        num_of_inlinks - parameter determining the number of inlinks for each 
                         page
        num_of_pages - parameter determining the number of pages needed

    '''
    titles = pd.read_csv(titles_file, delimiter = ' ', names = ['page title'])
    inlinks_sample = {}

    with open(json_file, 'r') as f:
        count = 0
        for key, val in json.load(f).items():
            if len(val) == num_of_inlinks:
                # convert key from line number (int) to pagename (str)
                key_name = titles.iloc[[int(key) - 1]].values[0][0]
                
                # parsing titles to remove unusual characters
                title = urllib.parse.unquote_plus(key_name)
                x = 0
                while '%' in title:
                    if x == 10:
                        break
                    title = urllib.parse.unquote_plus(title)
                    x += 1

                # convert each value from line number (int) to pagename (str)
                for v in val: 
                    val_name = titles.iloc[[int(v) - 1]].values[0][0]
                    # parsing title to remove unusual characters
                    title = urllib.parse.unquote_plus(val_name)
                    x = 0
                    while '%' in title:
                        if x == 10:
                            break
                        title = urllib.parse.unquote_plus(title)
                        x += 1

                    # appends val_name to key_name in inlink_sample dict
                    inlinks_sample[key_name] = inlinks_sample.get(key_name, \
                        []) + [val_name]
                count += 1

            if count == num_of_pages: 
                # once num_of_pages pages with the specified attributes
                # are obtained, dump the inlinks_sample dict and return None
                with open('samples/sample_' + str(num_of_inlinks) + '_' + \
                    str(num_of_pages), 'w') as f:
                    json.dump(inlinks_sample, f) 
                return None

    # if num_of_pages exceeds the number of pages that have num_of_inlinks
    # inlinks, then all the pages with num_of_inlinks are dumped into the 
    # proper file
    print('there are only {:} pages', count)
    with open('samples/sample_' + str(num_of_inlinks) + '_' + \
        str(num_of_pages), 'w') as f:
        json.dump(inlinks_sample, f) 
    return None


def one_to_five_inlinks_sample(num_of_inlinks, num_of_pages):
    '''
    Loads the proper json file with num_of_inlinks inlinks and num_of_pages
    pages, and returns the sample_dict.
    Inputs:
        num_of_inlinks - parameter determining the number of inlinks for each 
                         page
        num_of_pages - parameter determining the number of pages needed
    Output:
        sample_dict with num_of_pages pages with num_of_inlinks inlinks
    '''
    with open('samples/sample_' + str(num_of_inlinks) + '_' + \
        str(num_of_pages), 'r') as f:
        
        sample_dict = json.load(f) 
        return sample_dict


def two_inlinks_sample(json_file_two):
    '''
    Returns a sample dict with two inlinks each.
    Input:
        json_file_two - a json file with a dictionary sample of pages
                        with two inlinks each
    Output:
        a sample dict of pages with two inlinks as keys
    '''
    
    two_inlinks_sample = {}

    with open(json_file_two, 'r') as f:
        inlinks_dict = json.load(f)

    two_inlinks_sample['Wrestling_Slang'] = inlinks_dict['Wrestling_Slang']
    two_inlinks_sample['Concordia_University,_St._Paul'] = \
        inlinks_dict['Concordia_University,_St._Paul']
    two_inlinks_sample['A_Spaceman_Came_Travelling_(Christmas_Remix)'] = \
        inlinks_dict['A_Spaceman_Came_Travelling_(Christmas_Remix)']
    two_inlinks_sample['Transcendentals'] = inlinks_dict['Transcendentals']
    two_inlinks_sample['Platinum_Card'] = inlinks_dict['Platinum_Card']

    return two_inlinks_sample
