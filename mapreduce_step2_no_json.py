# To run the below code on local machine
# python3 mapreduce_step2_no_json.py -o "one-to-five" --link=samples/sample_5_5 --no-output --jobconf mapreduce.job.reduces=1 mrjob_test_output

# To run on S3: 
# python3 mapreduce_step2_no_json.py -r emr --cluster-id j-275HT34NGNX71 --link=samples/sample_5_20 --jobconf mapreduce.job.reduces=1 s3://wikitrafv2big/oct2008_en_step1/Oct_Total/ --output-dir=s3://wikitrafv2big/oct2008_en_step2/test/output_month_5_20/ --no-output

import mrjob
from mrjob.job import MRJob
import json
import re
import os


def one_to_five_inlinks_sample(sample_file):
	'''
	Function to get the pagename and links of interest that we are interested
	in testing. Another function creates a dictionary with the list of 
	pagenames of interest

	Input: 
		sample_file: JSON file containing pages of interest and corresponding 
			inlinks

	Output: 
		sample_dict: dictionary of pages of interest and corresponding inlinks
	'''
	with open(sample_file, 'r') as f:

		sample_dict = json.load(f) 
		return sample_dict



class PageName(MRJob):
	'''
	Class using MRJob to determine the determine all of the links associated
	with the Wikipedia pages of interest, and retrieve the pagename, datetime, 
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
			--link: file containing all Wikipedia pagenames of interest 
				and all associated inlinks
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
		self.interest = one_to_five_inlinks_sample(str(self.links))

		# All homepages of interest
		self.interest_keys = list(self.interest.keys())

		# All inlinks of interest. In form of a list
		self.interest_values = []
		for each in self.interest.values():
			self.interest_values += each


	def mapper(self, _, line):
		'''
		Function that yields pagenames that are links to the page of 
		interest, as well as pages of interest. Takes the output from 
		mapreduce_step1.py. 

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

		# fields[3] looks like: 25"			need to get rid of quotation mark.
		if re.findall("[0-9]+", fields[3]) != []:
			fields[3] = re.findall("[0-9]+", fields[3])[0]
		
			# Conditional if pagename is a homepage
			if title in self.interest_keys:
				yield title + "   " + fields[1] + "   " + fields[2], int(fields[3])

			# Conditional if pagename is an inlink
			elif title in self.interest_values:
				yield title + "   " + fields[1] + "   " + fields[2], int(fields[3])


	def combiner(self, relevant_line, bytes):
		'''
		Function to sum up bytes for each relevant Wikipedia page. Potentially 
		more than one entry for a given pagename and datetime. 

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

		# Do not care if bytes info = 0. Most likely error in data
		if relevant_bytes != 0: 
			output_line = fields[0] + "   " + fields[1] + "   " + fields[2] + "   " + str(relevant_bytes)

			yield None, output_line


if __name__ == '__main__':
	PageName.run()