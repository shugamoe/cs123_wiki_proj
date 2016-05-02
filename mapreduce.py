# To run code: python3 mapreduce.py pagecounts-20090826-200000
# python3 task1.py  --jobconf mapreduce.job.reduces=1 WhiteHouse-WAVES-Released-1210.csv

import mrjob
from mrjob.job import MRJob
import re

class EnglishEntries(MRJob):
	'''
	Class using MRJob to determine the English Wikipedia pages and the 
	total views for each page. 
	'''
	# To not display null values in the final output
	# OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

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

		# Only care about entries in English
		if re.findall('en', fields[0]) != []:
			yield fields[1], int(fields[2])

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
		if re.findall("avatar", page_name.lower()) != []:
			if re.findall("File:", page_name) == []: 
				yield page_name, sum(views)

if __name__ == '__main__':
	EnglishEntries.run()