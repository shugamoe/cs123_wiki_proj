# To exit VIM: Esc + :q!

# To run code. If 'mrjob_output' folder already exists, it gets overwritten. Data is mrjob_input (folder). With passthrough options: 
# python3 mapreduce_step2.py -o "Lower_Manhattan" --homepage="Lower_Manhattan" --link=links.txt --title=titles-sorted.txt --no-output --jobconf mapreduce.job.reduces=1 mrjob_test_output 
# python3 mapreduce_step2.py -o json_files/"Lower_Manhattan" --homepage="Lower_Manhattan" --link=inlinks_files/links --title=inlinks_files/titles-sorted.txt --no-output --jobconf mapreduce.job.reduces=1 oct2008_en 



# python3 mapreduce_step2_no_json.py -o "one-to-five" --link=one_to_five_inlinks.txt --no-output --jobconf mapreduce.job.reduces=1 mrjob_20081009_1st 

# python3 mapreduce_step2.py -o "one-to-five" --no-output --jobconf mapreduce.job.reduces=1 mrjob_test_output 


# Really good for AWS Map Reduce: https://dataiap.github.io/dataiap/day5/mapreduce
# MRJob documentation: https://media.readthedocs.org/pdf/mrjob/latest/mrjob.pdf

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import urllib.parse
import json
import os
import pandas as pd
import optparse

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



class PageName(MRJob):
	'''
	Class using MRJob to determine the determine all of the links associated
	with one Wikipedia page of interest, and retrieve the pagename, datetime, 
	pageviews, and bytes ratio (bytes_linked_page / bytes_interest_page) for 
	each datetime. This information is relevant to perform the regression 
	analyses. 

	PageName takes in output files from mapreduce_step1.py, meaning txt files
	with all potentially relevant English Wikipedia pagenames, datetimes, 
	bytes, and pageviews
	'''
	# To not display null values in the final output
	OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

	def configure_options(self):
		'''
		To provide passthrough options when calling PageName class in the 
		Terminal. Passthrough options include: 
			--homepage: String of Wikipedia pagename of interest
			--link: links.txt file containing all Wikipedia pagenames that are 
				links to any given Wikipedia page of interest
			--title: titles-sorted.txt file containing all potential Wikipedia 
				pagenames of interest
		'''
		super(PageName, self).configure_options()
		# self.add_passthrough_option('--homepage', type='str')
		self.add_file_option('--link')
		# self.add_file_option('--title')
		# self.add_passthrough_option('--hour', type='str')


	def mapper_init(self): 
		'''
		init to create a list of the pagenames of all links associated with 
		the page of interest

		Relevant output variable: 
			self.interest: List of all pagenames that link to page of interest
		'''
		# Set below variables equal to options written in Terminal command
		# self.page_of_interest = self.options.homepage
		self.links = self.options.link
		print(self.links)
		# self.titles = self.options.title

		# Calling helper to determine the pagenames linked to interest pagename
		self.interest = two_inlinks_sample(str(self.links))
		self.interest_keys = list(self.interest.keys())
		self.interest_values = []
		for each in self.interest.values():
			self.interest_values += each

		print(self.interest_keys)
		print(self.interest_values)
		# self.hour = self.options.hour


	def mapper(self, _, line):
		'''
		Function that yields pagenames that are links to the page of 
		interest. Takes the output from mapreduce_step1.py. 

		Input: 
			line: row of mapreduce_step1.py file, of form: 
				"pagename   datetime   pageviews   bytes"

		Outputs: 
			String of pagenames (and relevant details) that are links to page
				of interest. Of form: 
				"pagename   datetime   pageviews", bytes 
		'''
		# fields = [pagename, datestring, pageviews, bytes]
		fields = line.split("   ")
		# fields[0] looks like: "pagename 		need to get rid of quotation mark
		# pagename is often percent encoded, so need to remove this encoding
		title = fields[0][1:]

		# Conditional if pagename is a link to page of interest
		if title in self.interest_keys:
			# fields[3] looks like: 25"			need to get rid of quotation mark. 
			yield title + "   " + fields[1] + "   " + fields[2], int(fields[3][:-1])

		elif title in self.interest_values:
			# fields[3] looks like: 25"			need to get rid of quotation mark. 
			yield title + "   " + fields[1] + "   " + fields[2], int(fields[3][:-1])


	def combiner(self, relevant_line, bytes):
		'''
		Function to sum up bytes for each relevant Wikipedia page. 
		Should be removed, since all pagenames are unique in mapreduce_step1.py 
		output

		Inputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: bytes associated with relevant pagename

		Outputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: updated (probably not) bytes associated with relevant pagename
		'''
		yield relevant_line, sum(bytes)


	def reducer(self, relevant_line, bytes):
		'''
		Reducer to yield the relevant Wikipedia pages that link to the page 
		of interest. Also add to the self.page_of_interest_bytes dictionary
		if the pagename is the page of interest. Adding to the dictionary
		should look like: self.page_of_interest_bytes["datetime"] = bytes

		Inputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: bytes associated with relevant pagename

		Outputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: bytes associated with relevant pagename
		'''
		# fields[0] = pagename, fields[1] = datestring, fields[2] = pageviews
		fields = relevant_line.split("   ")
		relevant_bytes = sum(bytes)

		if relevant_bytes != 0: 
			output_line = fields[0] + "   " + fields[1] + "   " + fields[2] + "   " + str(relevant_bytes)

			yield None, output_line


if __name__ == '__main__':
	PageName.run()