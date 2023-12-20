import pandas as pd



import os

path = '/STFS0029M/PPG Extractor/2023-10-30'

files = os.listdir(path)

headers = pd.DataFrame(columns = ['file','headers'])


for i in range(0,len(files)):

	
	df = pd.read_csv(path+"/"+files[i],sep="|")
	
	headers.loc[i,'file'] = files[i]

	headers.loc[i,'headers'] = df.columns
	
headers.to_csv('headers.csv',index = False)

	
print('completed successfully')
	





