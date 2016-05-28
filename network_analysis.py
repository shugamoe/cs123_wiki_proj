import networkx as nx  
import pandas as pd
import matplotlib.pyplot as plt
import json
from inlinks import wiki_homepages

with open('links', 'r') as f:
	inlinks_dict = json.load(f)
G = nx.Graph()

def graph(json_file):

	G = nx.Graph()
	with open(json_file, 'r') as f:
		count = 0
		for key, values in json.load(f).items():
			for inlink in values:
				G.add_edge(key, inlink)
			count += 1

			if count % 10 == 0:
				print(count)
			if count == 100:
				break



	nx.draw(G, node_size = 100)
	plt.show()


def graph_from_page(G, pagename, num_of_steps, node_color, inlinks_dict = inlinks_dict, pagename_str = True):
	if pagename_str:
		titles = pd.read_csv('titles-sorted.txt', delimiter = ' ', names = ['page title'])
		line_index = titles[titles[[0]] == pagename].dropna().index.tolist()
		line_num = line_index[0] + 1

	else:
		line_num = pagename
	
	G.add_node(line_num, node_color = node_color)

	for inlink in inlinks_dict[str(line_num)]:
		G.add_edge(line_num, inlink)
		
		if num_of_steps > 1:
			graph_from_page(G, inlink, num_of_steps - 1, 'r', pagename_str = False)
		
	
	nx.draw(G, node_size = 100)
	plt.show()