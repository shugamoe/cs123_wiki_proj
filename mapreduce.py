# To run code and save output of reducers to plain text documents: 
# If 'mrjob_output' folder already exists, it gets overwritten
# python3 mapreduce.py -o 'mrjob_output' --no-output pagecounts-20090825-210000

# To run code on all files in a bucket in EMR, running on 1 computer: 
# python mr_wordcount.py --num-ec2-instances=1 --python-archive package.tar.gz -r emr -o 's3://dataiap-bobbyadusumilli-testbucket/output' --no-output 's3://dataiap-wikipedia/*'

# Really good for AWS Map Reduce: https://dataiap.github.io/dataiap/day5/mapreduce

# MRJob documentation: https://media.readthedocs.org/pdf/mrjob/latest/mrjob.pdf

import mrjob
from mrjob.job import MRJob
import re
import os
import datetime
import json
import gzip


# Figure out how to decompress files within MRJob


class EnglishEntries(MRJob):
	'''
	Class using MRJob to determine the English Wikipedia pages and the 
	total views for each page. 
	'''
	# To not display null values in the final output
	# INPUT_PROTOCOL = mrjob.protocol.
	OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

	def mapper(self, _, file):
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
		with gzip.open(file, 'rb') as f: 
			file_content = f.read()
			file_content = str(file_content).split("\\n")

		for line in file_content: 
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
		if re.findall("avatar", page_name.lower()) != []:
			if re.findall("File:", page_name) == []: 
				yield None, page_name + " " + str(sum(views))

if __name__ == '__main__':
	EnglishEntries.run()