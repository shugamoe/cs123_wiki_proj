import json
# from pathLib import Path


def two_inlinks_sample(json_file_one_to_five):
    '''
    Andy's function. Only choose certain homepages to do the regression
    analysis. 
    '''
    
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


def create_relevant(file, json_links):
    '''
    file example: 'one-to-five/part-00000'
    json_links example: 'one_to_five_inlinks.txt'

    Function to update file to get the appropriate MRJob data for the 
    regression analysis

    This updates the MRJob_Step2 regression output to only include 
    homepage and datetimes that are in the data, and also the the 
    information of the appropriate links
    '''

    # mrjob_outputs = [ptx_as_posix() for pth in Path.cwd().iterDir() if 'part-' in pth.stem]

    # Create a list of all infromation initial MRJob_Step2 output
    inlinks_mrjob = []
    with open(file, 'r') as f:
        for line in f: 
            inlinks_mrjob.append(line.strip()[1:-1])

    # Call function to get all homepages and links of interest
    inlinks_dict = two_inlinks_sample(json_links)
    # print(inlinks_dict)
    # print()

    inlinks_dict_keys = list(inlinks_dict.keys())
    # print(inlinks_dict_keys)
    # print()

    # Create a dictionary of homepages and datetimes in MRJob_Step2 data
    keys = {}
    # Create a list of relevant output for the MRJob_Step2 file
    final_lines = []
    new_inlinks_dict = {}

    for line in inlinks_mrjob:
        # fields = [pagename, datetime, pageviews, bytes]
        fields = line.split('   ')
        if fields[0] in inlinks_dict_keys:
            keys[fields[0]] = keys.get(fields[0], []) + [fields[1]]
            new_inlinks_dict[fields[0]] = new_inlinks_dict.get(fields[0], [])

    # print(list(keys.keys()))

    for line in inlinks_mrjob:
        # fields = [pagename, datetime, pageviews, bytes]
        fields = line.split('   ')

        # Include lines of the homepages
        if fields[0] in keys.keys():
            final_lines.append(line)

        # Include relevant links of the homepages
        else: 
            for key in keys.keys():
                if fields[0] in inlinks_dict[key]:
                    if fields[1] in keys[key]:
                        final_lines.append(line)
                        if fields[0] not in new_inlinks_dict[key]:
                            new_inlinks_dict[key] = new_inlinks_dict[key] + [fields[0]]
                        break

    # Overwrite the initial MRJob_Step2 file
    with open(file, 'w') as f:
        for line in final_lines: 
            f.write(line + "\n")

    return new_inlinks_dict
