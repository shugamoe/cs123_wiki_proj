# mapreduce_step1.py command on emr: 
# python3 mapreduce_step1.py -r emr s3://wikitrafv2big/oct2008_en/Week4/oct29_30/ --output-dir=s3://wikitrafv2big/oct2008_en_step1/Week4_Step1/oct29_30_step1/ --no-output

# To run the below code on local machine
# python3 mapreduce_step2_no_json.py -o "one-to-five" --link=one_to_five_inlinks.txt --no-output --jobconf mapreduce.job.reduces=1 mrjob_20081009_2nd 

# To run on S3: 
# python3 mapreduce_step2_no_json.py -r emr --link=one_to_five_inlinks.txt --jobconf mapreduce.job.reduces=1 s3://wikitrafv2big/oct2008_en_step1/Week4_Step1/oct29_30_step1/ --output-dir=s3://wikitrafv2big/oct2008_en_step2/test/output/ --no-output

# python3 mapreduce_step2_no_json.py -r emr --link=one_to_five_inlinks.txt --jobconf mapreduce.job.reduces=1 s3://wikitrafv2big/oct2008_en_step1/Week4_Step1/oct29_30_step1/ --output-dir=s3://wikitrafv2big/oct2008_en_step2/test/output/ --no-output

# Data size = 9.26GB + 10.09GB + 9.18GB + 13.62GB = 42.15GB
# Folders = 104 + 62 + 81 + 106 = 353 Files

import mrjob
from mrjob.job import MRJob
import json
# import pandas as pd


def two_inlinks_sample(json_file_one_to_five):
	'''
	Andy's function for a two link sample
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



class PageName(MRJob):
	'''
	Class using MRJob to determine the determine all of the links associated
	with one Wikipedia page of interest, and retrieve the pagename, datetime, 
	pageviews, and bytes for each datetime. This info is relevant to perform 
	the regression analyses. This function doesn't use a json file to store 
	the homepage bytes numbers, unlike the mapreduce_step2.py, which is run
	on local machine. Functions for statistical analysis will calculate bytes
	ratios. 

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
			--link: links.txt file containing all Wikipedia pagenames that are 
				links to any given Wikipedia page of interest
		'''
		super(PageName, self).configure_options()
		self.add_file_option('--link')


	def mapper_init(self): 
		'''
		init to create a list of the pagenames of all links associated with 
		the page of interest

		Relevant output variable: 
			self.interest: List of all pagenames that link to page of interest
		'''
		# Set below variables equal to options written in Terminal command
		self.links = self.options.link

		# Calling helper to determine the pagenames linked to interest pagename
		self.interest = two_inlinks_sample(str(self.links))

		# All homepages of interest
		self.interest_keys = list(self.interest.keys())

		# All inlinks of interest
		self.interest_values = []
		for each in self.interest.values():
			self.interest_values += each


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
		title = fields[0][1:]

		# Conditional if pagename is a homepage
		if title in self.interest_keys:
			# fields[3] looks like: 25"			need to get rid of quotation mark. 
			yield title + "   " + fields[1] + "   " + fields[2], int(fields[3][:-1])

		# Conditional if pagename is an inlink
		elif title in self.interest_values:
			# fields[3] looks like: 2500"			need to get rid of quotation mark. 
			yield title + "   " + fields[1] + "   " + fields[2], int(fields[3][:-1])


	def combiner(self, relevant_line, bytes):
		'''
		Function to sum up bytes for each relevant Wikipedia page. 
		Realistically bytes data should not change since mostly unique entries. 

		Inputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: bytes associated with relevant pagename

		Outputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: updated bytes associated with relevant pagename
		'''
		yield relevant_line, sum(bytes)


	def reducer(self, relevant_line, bytes):
		'''
		Reducer to yield the relevant Wikipedia pages that link to the pages 
		of interest, and pages of interest themselves. 

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

		# Do not care if bytes info = 0. Most likely problem in data
		if relevant_bytes != 0: 
			output_line = fields[0] + "   " + fields[1] + "   " + \
			fields[2] + "   " + str(relevant_bytes)

			yield None, output_line


if __name__ == '__main__':
	PageName.run()