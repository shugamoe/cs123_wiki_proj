import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt

df = pd.read_csv('sample_data/pagecounts-20090826-210000', delimiter = ' ', \
	columns = list('page name', 'datetime', 'page views'))

