import networkx as nx  
import pandas as pd
import matplotlib.pyplot as plt
import json
from inlinks import wiki_homepages

with open('links', 'r') as f:
	inlinks_dict = json.load(f)
G = nx.Graph()

colors = ['b', 'r', 'g', 'c', 'm', 'y']

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


def graph_from_page(G, pagename, num_of_steps, inlinks_dict = inlinks_dict, 
	pagename_bytes = []): #  14255(Dead...),53857 (Sickx), 15168 (Nigga_Deep),  13159(If these walls),  243673 (Brotha...),  16539 (Loaded), 


	titles = pd.read_csv('titles-sorted.txt', delimiter = ' ', names = ['page title'])
	line_index = titles[titles[[0]] == pagename].dropna().index.tolist()
	line_num = str(line_index[0] + 1)
	
	labels = {}

	G.add_node(line_num)
	G.node[line_num]['color'] = colors[0]
	labels[line_num] = pagename

	for inlink in inlinks_dict.get(line_num, []):
		G.add_edge(line_num, inlink)
		G.node[inlink]['color'] = colors[1]
		labels[inlink] = titles.iloc[[int(inlink) - 1]].values[0][0]

		if num_of_steps > 1:
			G = graph_from_page_helper(G, inlink, num_of_steps - 1, 
				1)	
	pos = nx.spring_layout(G)
	if len(pagename_bytes) == 0:
		nx.draw(G, node_color = [G.node[node].get('color') for node in G], node_size = 1000)
	else:
		nx.draw(G, node_color = [G.node[node].get('color') for node in G], node_size = pagename_bytes)
	nx.draw_networkx_labels(G, pos, labels, font_size = 18)
	manager = plt.get_current_fig_manager()
	manager.window.showMaximized() 
	plt.show()


def graph_from_page_helper(G, line_num, num_of_steps, color_ind):

	for inlink in inlinks_dict.get(line_num, []):
		G.add_edge(line_num, inlink)

		if G.node[inlink].get('color', []) == []:			
			G.node[inlink]['color'] = colors[color_ind + 1]

	for inlink in inlinks_dict.get(line_num, []):

		if num_of_steps > 1:
			G = graph_from_page_helper(G, inlink, num_of_steps - 1, 
				color_ind + 1)

	return G
