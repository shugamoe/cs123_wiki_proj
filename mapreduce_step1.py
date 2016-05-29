# CS12300 Big Data Project - Julian McClellan, Andy Zhu, Bobby Adusumilli

# Run on local machine: If 'mrjob_output' folder already exists, it gets overwritten
# python3 mapreduce_step1.py -o 'mrjob_20081019_2nd' --no-output mrjob_small_input

# Run using AWS: 
# python3 mapreduce_step1.py -r emr s3://wikitrafv2big/oct2008_en/Week4/oct29_30/ --output-dir=s3://wikitrafv2big/oct2008_en_step1/Week4_Step1/oct29_30_step1/ --no-output

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import datetime
import urllib.parse

class RelevantEntries(MRJob):
	'''
	Class using MRJob to determine the relevant English Wikipedia pages and 
	the total views and bytes for each hour for each page. Takes in lines 
	from filenames such as "pagecounts-20081001-060000"

	Input looks like: "en Barack_Obama 997 123091092" 
	Format: "language pagename pageviews bytes"

	Output looks like: "Barack_Obama   2009/08/26/23   997   123091092 "		
	Format: "pagename   datetime   pageviews   bytes"
	'''
	# To not display null values in the final output
	OUTPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

	def mapper_init(self):
		'''
		Input to remove pagenames that do not have inlinks associated with them. 
		For example, images cannot lead to other links in Wikipedia, so any 
		pagename starting with "image:" can be removed. Also remove percent
		encoding from pagenames

		Relevant Output: 
			self.remove: string of beginnings of pagenames signifying which 
			pagenames can be removed. 
		'''
		self.remove_list = ["image:", "special:", "user:", "user_talk:", \
		"wiki/", "#", "image_talk:", "w/index", "category:", "category_talk:", \
		"portal:", "wikipedia:", "talk:", "template:", "wikibooks:", \
		"wikipedia_talk:", "title=", "search=", "template_talk:", "help:", \
		"help_talk:", "wikiquote:", "wiktionary:", "wikisource:", "category%3a"]
		self.remove = "|".join(self.remove_list)

	def mapper(self, _, line):
		'''
		Mapper function that will take each line of the space-separated 
		file and yield each English Wikipedia page's name, number of bytes, 
		relevant hour, and total views for that page in that hour. Only should
		yield relevant pagenames that can have inlinks. 

		Input: 
			line: row of a space-separated file. line is of form: 
			"language pagename pageviews bytes"

		Outputs: 
			Relevant English Wikipedia page name and datetime
			List of total views and bytes of that Wikipedia page
		'''
		# fields = ["en", pagename, pageviews, bytes]
		fields = line.split(" ")
		filename = os.environ["mapreduce_map_input_file"]
		date = re.findall("[0-9]+-[0-9]+", filename)[0]
		date = datetime.datetime.strptime(date, "%Y%m%d-%H%M%S")
		date = datetime.datetime.strftime(date, "%Y/%m/%d/%H")

		title = urllib.parse.unquote_plus(fields[1])
		
		# Sometimes percent encoding needs greater than 1 time to remove
		x = 0
		# Chose 10 to ensure percent encodings disappear
		while "%" in title: 
			if x == 10:
				break
			# command to remove percent encodings
			title = urllib.parse.unquote_plus(title) 
			x += 1

		if re.findall(self.remove, title.lower()) == []:

			# Sometimes data is corrupted
			try: 
				# Sometimes for title and datetime, there are numerous entries. 
				# output is: "pagename   datetime", [pageviews, bytes]
				yield title + "   " + date, [int(fields[2]), int(fields[3])]

			except: 
				print("This line messed up the code: " + str(line))

	def combiner(self, pagename, numbers):
		'''
		Combiner function to sum up the total number of views and bytes 
		for each Wikipedia page. Sometimes there are >1 entries for one
		Wikipedia pagename for given datetime, so we sum the pageviews and 
		bytes

		Inputs: 
			pagename: Wikipedia pagename and relevant datetime
			views: pseudo-list of number of views and bytes of Wikipedia 
				page 

		Outputs: 
			String of relevant English Wikipedia page name and datetime
			List of total view and bytes of that Wikipedia page and datetime
		'''
		page_numbers = numbers
		pageviews = 0
		bytes = 0

		for each in page_numbers:
			pageviews += each[0] 
			bytes += each[1]

		yield pagename, [pageviews, bytes]

		

	def reducer(self, pagename, numbers):
		'''
		Reducer function to yield the Enligh Wikipedia pagenames that can have
		inlinks. String also includes datetime, bytes, and pageviews. 

		Inputs: 
			pagename: String of Wikipedia pagename and datetime
			views: pseudo-list of number of views and bytes of page 

		Outputs: 
			String of relevant English Wikipedia pagename, datetime, bytes, 
			and pageviews. Output is of form: 
			"pagename   datetime   pageviews   bytes"
		'''
		page_numbers = numbers
		pageviews = 0
		bytes = 0

		for each in page_numbers:
			pageviews += each[0]
			bytes += each[1]

		yield None, pagename + "   " + str(pageviews) + "   " + str(bytes)


if __name__ == '__main__':
	RelevantEntries.run()