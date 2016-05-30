Github Repository: cs123_wiki_proj
CS12300 Big Data Project - Julian McClellan, Andy Zhu, Bobby Adusumilli

mapreduce_step1.py - MRJob function to take raw Wikipedia data and remove
unnecessary lines that cannot have inlinks, as well as remove percent 
encoding and put the relevant datetime for each entry. Output went to 
s3://wikitrafv2big/oct2008_en_step1/

mapreduce_step2.py - MRJob function that takes the output from 
mapreduce_step1.py, as well as a Wikipedia pagename, and finds all of
the hourly pageviews and bytes for each homepage and the corresponding 
inlinks. This incorporates a JSON file for the homepage to calculate 
the appropriate bytes ratio for each page. We used this initially for 
our data analysis, but realized using mapreduce_step2_no_json.py would 
be more efficient. 

mapreduce_step2_no_json.py - MRJOB function that takes the output from
mapreduce_step1.py, as well as a Wikipedia pagename, and finds all of the 
hourly pageviews and bytes for each homepage and the corresponding inlinks. 
The output went to s3://wikitrafv2big/oct2008_en_step2/test/. Output then 
processed by batch_to_csv.py to perform data analysis in R. 

mrjob_avg_bytes.py - MRJob function to calculate the average bytes value
for a given pagename over a given dataset. 

opening_wikipedia_files.txt - Text file on how to open Wikipedia data 
from AWS and transfer it to a local machine. Relevant for when we 
initially started the project. 

inverse.py - Contains a function that uses links.txt which lists all pages and 
their outlinks, and effectively creates a dictionary that inverts it. So, the 
new dictionary lists all pages and their inlinks. This is then dumped to a 
file in the directory called 'links'

inlinks.py - Contains wiki_homepages functionthat returns a list of inlinks 
given a pagename by using the 'links' json-encoded dict and the 
titles-sorted.txt file to convert page numbers into actual titles. This file
also contains various functions that either dump a subset of 'links', where 
each page contains one to five inlinks, or loads a json dump and returns a 
small dict with page titles and specified inlinks for each.

network_analysis.py - Uses 'links' to create certain network graphs. After
specifying a pagename, the graph_from_page function creates a set of nodes
starting with pagename, and recursively branches out from each inlink to create
inlinks of inlinks, etc. depending on the num_of_steps parameter.