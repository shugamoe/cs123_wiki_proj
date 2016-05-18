import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import os
from collections import defaultdict
import shlex
import fileinput

def wiki_links(pagename):

    fileinput.close()
    file_input = fileinput.input('titles-sorted.txt')
    line_num = 1
    line_number = 0

    for line in file_input: # iterate through each line to obtain the line number
        
        if line.strip('\n') == pagename:
            line_number = line_num # line number found
            break

        else:
            line_num += 1

    fileinput.close()
    home_ind_list = []


    file_input = fileinput.input('links-simple-sorted.txt')
    for line in file_input:
        line = line.split(': ')

        # if home link is line_number
        if int(line[0]) == line_number: 
            link_ind_list = line[1].strip('\n').split(' ') #list of links

        # if line_number is in list of links
        if str(line_number) in line[1].strip('\n').split(' '): 
            home_ind_list.append(line[0])

    fileinput.close()

    link_titles = []
    home_titles = []

    fileinput.close()
    file_input = fileinput.input('titles-sorted.txt')
    line_num = 1

    for line in file_input:

        if str(line_num) in link_ind_list:
            link_titles.append(line.strip('\n'))

        if str(line_num) in home_ind_list:
            home_titles.append(line.strip('\n'))

        line_num += 1

    return link_titles, home_titles


