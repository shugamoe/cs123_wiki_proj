import json


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


def create_relevant():
    inlinks_mrjob = []
    with open('one-to-five/part-00000', 'r') as f:
        for line in f: 
            inlinks_mrjob.append(line.strip()[1:-1])

    # print(inlinks_mrjob)

    inlinks_dict = two_inlinks_sample('one_to_five_inlinks.txt')

    inlinks_dict_keys = list(inlinks_dict.keys())

    keys = {}
    final_lines = []

    for line in inlinks_mrjob:
        # fields = [pagename, datetime, pageviews, bytes]
        fields = line.split('   ')
        # print(fields)
        if fields[0] in inlinks_dict_keys:
            keys[fields[0]] = keys.get(fields[0], []) + [fields[1]]

    for line in inlinks_mrjob:
        fields = line.split('   ')

        if fields[0] in keys.keys():
            final_lines.append(line)

        else: 
            for key in keys.keys():
                if fields[0] in inlinks_dict[key]:
                    if fields[1] in keys[key]:
                        final_lines.append(line)
                        break

    with open('one-to-five/part-00000', 'w') as f:
        for line in final_lines: 
            f.write(line + "\n")


    # return final_lines

