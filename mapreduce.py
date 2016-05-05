# To run code: python3 mapreduce.py pagecounts-20090826-200000

# pip3 install --user mr3px.csvprotocol

import mrjob
from mrjob.job import MRJob
import re
import os
import datetime
import json


class EnglishEntries(MRJob):
	'''
	Class using MRJob to determine the English Wikipedia pages and the 
	total views for each page. 
	'''
	# To not display null values in the final output
	OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

	def mapper(self, _, line):
		'''
		Starter function that will take each line of the space-separated 
		file and yield each English Wikipedia's page name and the number 
		of views for that page 

		Input: 
			line: row of a space-separated file

		Outputs: 
			Wikipedia page name
			Total views of that Wikipedia page
		'''
		# fields[0] = language, fields[1] = page name, fields[2] = page views, fields[3] = bytes
		fields = line.split(' ')
		filename = os.environ["mapreduce_map_input_file"]
		date = re.findall("[0-9]+-[0-9]+", filename)[0]
		date = datetime.datetime.strptime(date, "%Y%m%d-%H%M%S")
		date = datetime.datetime.strftime(date, "%Y/%m/%d/%H")

		# Only care about entries in English
		if re.findall('en', fields[0]) != []:
			yield fields[1] + " " + date, int(fields[2])

	def combiner(self, page_name, views):
		'''
		Combiner function to sum up the total number of views for each 
		Wikipedia page

		Inputs: 
			page_name: name of Wikipedia page
			visits: pseudo-list of number of views of Wikipedia page 

		Outputs: 
			Wikipedia page name
			Total views of that Wikipedia page
		'''
		yield page_name, sum(views)

	def reducer(self, page_name, views):
		'''
		Reducer function to yield the Wikipedia pages in English and the 
		number of views by people 

		Inputs: 
			page_name: name of Wikipedia page
			visits: pseudo-list of number of views of Wikipedia page 

		Outputs: 
			Wikipedia page name
			Total views of that Wikipedia page
		'''
		# print(os.environ["mapreduce_map_input_file"])
		if re.findall("avatar", page_name[0].lower()) != []:
			if re.findall("File:", page_name[0]) == []: 
				with open('mrjob_output.txt', 'w') as outfile: 
					json.dump(page_name + " " + sum(views), outfile)
				# yield (page_name, sum(views))

if __name__ == '__main__':
	EnglishEntries.run()