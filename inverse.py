import json

def invert():
	'''
	Takes a text file where each line contains a page number and a list of 
	outlinks from that page, and dumps a new dictionary with page numbers as 
	keys, and inlinks as values, into another file. This is a one-time process.
	'''

	d = {}
	with open('links-simple-sorted.txt', 'r') as f:
		lines = f.readlines()
		for line in lines:
			line = line.split(': ')
			# iterates through each outlink, and appends the page number 
			# of the original link as a value (inlink) of d[outlink]
			for link in line[1].strip('\n').split(' '): 
				d[link] = d.get(link, []) + [line[0]]
			# checks status every so often
			print(line[0] + ' of ~5,706,900 done') 

	with open('links', 'wb') as f:
		json.dump(d, f) # dumps d into 'links'


def invert_subset():
	'''
	Inverts a subset of the data, and dumps the dictionary of inlinks into 
	another file.
	'''
	d = {}
	with open('links-simple-sorted.txt', 'r') as f:
		lines = f.readlines()
		line_num = 0
		for line in lines:
			line_num += 1
			line = line.split(': ')
			for link in line[1].strip('\n').split(' '):
				d[link] = d.get(link, []) + [line[0]]
			if line_num > 1000: # inverts 1,000 lines
				break

	with open('links_subset', 'w') as f:
		json.dump(d, f)