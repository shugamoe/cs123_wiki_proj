# To exit VIM: Esc + :q!

# To run code. If 'mrjob_output' folder already exists, it gets overwritten. Data is mrjob_input (folder). With passthrough options: 
# python3 mapreduce_step2.py -o "Lower_Manhattan" --homepage="Lower_Manhattan" --link=links.txt --title=titles-sorted.txt --no-output --jobconf mapreduce.job.reduces=1 mrjob_test_output 

# To run code on all files in a bucket in EMR, running on 1 computer: 
# python mr_wordcount.py --num-ec2-instances=1 --python-archive package.tar.gz -r emr -o 's3://dataiap-bobbyadusumilli-testbucket/output' --no-output 's3://dataiap-wikipedia/*'

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
		self.add_passthrough_option('--homepage', type='str')
		self.add_file_option('--link')
		self.add_file_option('--title')
		# self.add_passthrough_option('--hour', type='str')


	def mapper_init_first(self): 
		'''
		init to create a list of the pagenames of all links associated with 
		the page of interest

		Relevant output variable: 
			self.interest: List of all pagenames that link to page of interest
		'''
		# Set below variables equal to options written in Terminal command
		self.page_of_interest = self.options.homepage
		self.links = self.options.link
		self.titles = self.options.title

		# Calling helper to determine the pagenames linked to interest pagename
		self.interest = wiki_homepages(self.page_of_interest, str(self.links), str(self.titles))
		print(self.interest)
		# self.hour = self.options.hour


	def mapper_first(self, _, line):
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
		title = urllib.parse.unquote_plus(fields[0])[1:]

		# Conditional if pagename is a link to page of interest
		if title in self.interest:
			# fields[3] looks like: 25"			need to get rid of quotation mark. 
			yield title + "   " + fields[1] + "   " + fields[2], int(fields[3][:-1])

		# Include info for page of interest
		elif title == self.page_of_interest: 
			yield title + "   " + fields[1] + "   " + fields[2], int(fields[3][:-1])


	def combiner_first(self, relevant_line, bytes):
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


	def reducer_init_first(self):
		'''
		init to create JSON file for page of interest that will store a 
		dictionary for the page of interest of each datetime and bytes 
		associated with that page of interest at that datetime. Necessary 
		for calculating bytes ratio in next MRJob step. 

		Relevant variables: 
			self.page_of_interest_bytes: Dictionary that will house datetimes
				and associated bytes for the page of interest
		'''
		self.page_of_interest = self.options.homepage
		self.page_of_interest_bytes = {}

		with open("/home/badusumilli/cs123_wiki_proj/" + str(self.page_of_interest) + ".json", "w") as outfile:
			json.dump({}, outfile)


	def reducer_first(self, relevant_line, bytes):
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
		if fields[0] == self.page_of_interest:
			if relevant_bytes != 0: 
				self.page_of_interest_bytes[fields[1]] = relevant_bytes

		yield relevant_line, relevant_bytes


	def reducer_final_first(self): 
		'''
		Add the self.page_of_interest_bytes dictionary to a JSON file that 
		allows for the MRJob Step1 dictionary of (datetime, bytes) pairs 
		to be used on MRJob Step2. 

		Relevant file: 
			"/home/badusumilli/cs123_wiki_proj/" + str(self.page_of_interest) + ".json"
		'''
		with open("/home/badusumilli/cs123_wiki_proj/" + str(self.page_of_interest) + ".json") as f:
		    catalog = json.load(f)

		with open("/home/badusumilli/cs123_wiki_proj/" + str(self.page_of_interest) + ".json", "w") as outfile:
			catalog.update(self.page_of_interest_bytes)
			json.dump(catalog, outfile)


	def reducer_init_last(self):
		'''
		Reducer init of Step2 that creates local variable of the dictionary 
		for the page of interest that includes (datetime, bytes) pairs. 

		Relevant variable: 
			self.page_of_interest_bytes: dictionary for page of interest
		'''
		self.page_of_interest = self.options.homepage
		with open("/home/badusumilli/cs123_wiki_proj/" + str(self.page_of_interest) + ".json") as f:
			self.page_of_interest_bytes = json.load(f)


	def reducer_last(self, relevant_line, bytes):
		'''
		Reducer to yield the page of interest and all pagenames that link 
		(including all details). 

		If pagename = page of interest, it is necessary to include overall
		bytes to assist in regressions. Output is: 
			"pagename   datetime   pageviews   bytes_ratio   bytes"

		If pagename != page of interest, output is:
			"pagename   datetime   pageviews   bytes_ratio"
		'''
		interest_bytes = sum(bytes)
		# fields = [pagename, datetime, pageviews]
		fields = relevant_line.split("   ")

		if fields[1] in self.page_of_interest_bytes.keys():
			bytes_ratio = round(interest_bytes / self.page_of_interest_bytes[fields[1]], 2)
			# output_line looks like: pagename   datetime   pageviews   bytes_ratio
			if fields[0] != self.page_of_interest: 
				output_line = fields[0] + "   " + fields[1] + "   " + fields[2] + "   " + str(bytes_ratio)
			else: 
				output_line = fields[0] + "   " + fields[1] + "   " + fields[2] + "   " + str(bytes_ratio) + "   " + str(interest_bytes)

			yield None, output_line	


	def steps(self):
		'''
		2 MRJob steps are needed in order to determine the bytes ratio for
		each page associated with the page of interest
		'''
		return [
		  MRStep(mapper_init=self.mapper_init_first,
		  		 mapper=self.mapper_first,
		         combiner=self.combiner_first,
		         reducer_init=self.reducer_init_first,
		         reducer=self.reducer_first,
		         reducer_final=self.reducer_final_first),

		  MRStep(reducer_init=self.reducer_init_last,
		  		 reducer=self.reducer_last)
		]


if __name__ == '__main__':
	PageName.run()