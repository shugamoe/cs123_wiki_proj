import networkx as nx  
import pandas as pd
import matplotlib.pyplot as plt
import json
from inlinks import wiki_homepages

with open('links', 'r') as f:
	inlinks_dict = json.load(f) # loads dict of all pages and their inlinks
G = nx.Graph()

colors = ['b', 'r', 'g', 'c', 'm', 'y'] # list of colors of nodes

def graph(json_file):
	'''
	Graphs all wikipedia pages as nodes and all links to other pages as edges
	from json_file.
	Input:
		json_file - json_file containing all pages and their inlinks
	Output:
		Graph of nodes (pages) and edges (links) to other nodes
	'''

	G = nx.Graph()
	with open(json_file, 'r') as f:
		count = 0
		for key, values in json.load(f).items():
			for inlink in values:
				# Adds edges between each key and each inlink for that key
				G.add_edge(key, inlink) 
			count += 1

			if count % 10 == 0: # print statements to check status
				print(count)

	nx.draw(G, node_size = 100)
	plt.show()


def graph_from_page(G, pagename, num_of_steps, inlinks_dict = inlinks_dict, 
	pagename_bytes = []): 
	'''
	Graphs a set of nodes and edges that start with pagename, and branches
	out to inlinks, with each inlink branching out and so on based on 
	num_of_steps. Pagename_bytes determines the size of each node.
	Inputs:
		G - a networkx Graph object
		pagename - the page of interest
		num_of_steps - the number of times to 'branch out' of an inlink, i.e.
					   if num_of_steps = 2, all inlinks of inlinks would be 
					   graphed.
		inlinks_dict - dict of pages and their inlinks
		pagename_bytes - a list of the number of bytes of each page
	Output:
		A display of a network graph of all relevant nodes and edges
	'''
	titles = pd.read_csv('titles-sorted.txt', delimiter = ' ', names = 
		['page title'])
	line_index = titles[titles[[0]] == pagename].dropna().index.tolist()
	line_num = str(line_index[0] + 1)
	
	labels = {}

	G.add_node(line_num)
	G.node[line_num]['color'] = colors[0]
	labels[line_num] = pagename

	# Adds an edge for each inlink to key, and a color to the inlink node
	for inlink in inlinks_dict.get(line_num, []): 
		G.add_edge(line_num, inlink)
		G.node[inlink]['color'] = colors[1]

		# calls helper function to graph inlinks of inlinks. The default
		# color of each node also changes
		if num_of_steps > 1:
			G = graph_from_page_helper(G, inlink, num_of_steps - 1, 
				1)	
	if len(pagename_bytes) == 0:
		nx.draw(G, node_color = [G.node[node].get('color') for node in G], node_size = 300)
	else:
		nx.draw(G, node_color = [G.node[node].get('color') for node in G], 
			node_size = pagename_bytes)
	
	pos = nx.spring_layout(G)
	nx.draw_networkx_labels(G, pos, labels, font_size = 18)
	manager = plt.get_current_fig_manager()
	manager.window.showMaximized() 
	plt.show()


def graph_from_page_helper(G, line_num, num_of_steps, color_ind):
	'''
	Helper function to graph nodes and edges for inlinks of inlinks, or 
	inlinks of inlinks of inlinks, etc.
	Inputs:
		G - a networkx Graph object
		line_num - the line number of a page of interest (an inlink from the 
			previous function)
		num_of_steps - the number of times to 'branch out' of an inlink, i.e.
					   if num_of_steps = 2, all inlinks of inlinks would be 
					   graphed.
		color_ind - the default color of each node in this layer of inlinks
	Output:
		A display of a network graph of all relevant nodes and edges
	'''

	for inlink in inlinks_dict.get(line_num, []):
		# Adds edge for each inlink to key, and adds the default color for
		# each inlink node that doesn't already have a color. This prevents
		# recoloring a node.
		G.add_edge(line_num, inlink)

		if G.node[inlink].get('color', []) == []:			
			G.node[inlink]['color'] = colors[color_ind + 1]

	for inlink in inlinks_dict.get(line_num, []):
		# Recursively calls function based on num_of_steps. Each node in this 
		# layer has an established color by this point.
		if num_of_steps > 1:
			G = graph_from_page_helper(G, inlink, num_of_steps - 1, 
				color_ind + 1)

	return G
