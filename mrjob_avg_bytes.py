# To run this py file: 
# python3 mrjob_avg_bytes.py -r emr --cluster-id j-275HT34NGNX71 --pages="BioShock" --jobconf mapreduce.job.reduces=1 s3://wikitrafv2big/oct2008_en_step1/Oct_Total/ --output-dir=s3://wikitrafv2big/oct2008_en_step2/bioshock/ --no-output


import mrjob
from mrjob.job import MRJob
import json
import re
import os


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
		self.add_passthrough_option('--pages', type='str')


	def mapper_init(self): 
		'''
		init to create a list of the pagenames of all links associated with 
		the page of interest

		Relevant output variable: 
			self.interest: List of all pagenames that link to page of interest
		'''
		# Set below variables equal to options written in Terminal command
		self.pages = self.options.pages

		# self.list = []


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

		if title == self.pages:

			# fields[3] looks like: 25"			need to get rid of quotation mark.
			if re.findall("[0-9]+", fields[3]) != []:
				fields[3] = re.findall("[0-9]+", fields[3])[0]

				yield title + "   " + fields[1] + "   " + fields[2], int(fields[3])



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


	def reducer_init(self):
		self.pages = self.options.pages
		# bytes_dict = {}
		# word_frequency = {}

		self.bytes = 0
		self.frequency = 0


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

		# bytes_dict[fields[0]] = bytes_dict.get(fields[0], 0) + relevant_bytes
		# word_frequency[fields[0]] = word_frequency.get(fields[0], 0) + 1

		self.bytes += relevant_bytes
		self.frequency += 1


	def reducer_final(self):
		# for page in self.pages: 
		# output = bytes_dict.get(page, 0) / word_frequency.get(page, 1)
		if self.frequency == 0:
			output = 0

		else: 
			output = self.bytes / self.frequency

		yield None, self.pages + "   " + str(output) 



if __name__ == '__main__':
	PageName.run()