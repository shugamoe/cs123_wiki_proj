# CS12300 Big Data Project - Julian McClellan, Andy Zhu, Bobby Adusumilli

# To run this py file using out AWS data: 
# python3 mrjob_avg_bytes.py -r emr --cluster-id j-275HT34NGNX71 --pages="BioShock" --jobconf mapreduce.job.reduces=1 s3://wikitrafv2big/oct2008_en_step1/Oct_Total/ --output-dir=s3://wikitrafv2big/oct2008_en_step2/bioshock/ --no-output


import mrjob
from mrjob.job import MRJob
import json
import re
import os


class PageName(MRJob):
	'''
	Class using MRJob to determine the average bytes value of one page over 
	all of the time in one dataset.  

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
			--pages: Pagename of interest
		'''
		super(PageName, self).configure_options()
		self.add_passthrough_option('--pages', type='str')


	def mapper_init(self): 
		'''
		init to specify the page of interest

		Relevant output variable: 
			self.pages: pagename of interest
		'''
		# Set below variable equal to options written in Terminal command
		self.pages = self.options.pages


	def mapper(self, _, line):
		'''
		Function that yields all hourly data for pagename of interest 

		Input: 
			line: row of mapreduce_step1.py file, of form: 
				"pagename   datetime   pageviews   bytes"

		Outputs: 
			String of pagename of interest (and relevant details). Of form: 
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
		Function to sum up bytes for page of interest for each hour

		Inputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: bytes associated with relevant pagename and datetime

		Outputs: 
			relevant_line: Relevant pagename, associated datetime, and pageviews
			bytes: updated bytes associated with relevant pagename and datetime
		'''
		yield relevant_line, sum(bytes)


	def reducer_init(self):
		'''
		Specify variables to help calculate the average bytes value for the 
		page of interest over the dataset
		'''
		self.pages = self.options.pages

		self.bytes = 0
		self.frequency = 0


	def reducer(self, relevant_line, bytes):
		'''
		Reducer to yield the calculate the sum of the bytes of the page of 
		interest over the dataset, as well as count the frequency of how many 
		times the pagename showed up in the hourly dataset

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

		self.bytes += relevant_bytes
		self.frequency += 1


	def reducer_final(self):
		'''
		Calculate and return the average bytes of the page of interest over 
		the given dataset
		'''
		# Specify pagename not in data 
		if self.frequency == 0:
			output = 0

		else: 
			output = self.bytes / self.frequency

		yield None, self.pages + "   " + str(output) 



if __name__ == '__main__':
	PageName.run()