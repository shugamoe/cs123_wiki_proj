import json

def invert():
	d = {}
	with open('links-simple-sorted.txt', 'r') as f:
		lines = f.readlines()
		for line in lines:
			line = line.split(': ')
			for link in line[1].strip('\n').split(' '):
				d[link] = d.get(link, []) + [line[0]]
			print(line[0] + ' of 5,706,900 done')

	with open('links', 'wb') as f:
		json.dump(d, f)


def invert_subset():
	d = {}
	with open('links-simple-sorted.txt', 'r') as f:
		lines = f.readlines()
		line_num = 0
		for line in lines:
			line_num += 1
			line = line.split(': ')
			for link in line[1].strip('\n').split(' '):
				d[link] = d.get(link, []) + [line[0]]
			if line_num > 1000:
				break

	with open('links_subset', 'w') as f:
		json.dump(d, f)