# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 19:13:28 2023

@author: CBT
"""

import pandas as pd

import hashlib

import json

from kafka import KafkaProducer

config = pd.read_excel('config.xlsx',engine = 'openpyxl')

config = dict(list(zip(config['key'],config['value'])))

import requests

#Reading files

import os

def list_all_files_in_drive(drive):

    all_files = []

    for root, dirs, files in os.walk(drive):

        for file in files:

            file_path = os.path.join(root, file)

            all_files.append(file_path)

    return all_files

drive_to_list = config['source_path']

files_in_drive = list_all_files_in_drive(drive_to_list)

file1 = []

for file in files_in_drive:
    
    list1 = file.split('//')
    
    
    list1.pop()
    
    list1 = '/'.join(list1)

                       
    file1.append(list1)

file1 = set(file1)




for i in file1:
    

    if (config['CDMS_file1']) in os.listdir(i):
        
        print(i)
            
        file1 = pd.read_csv(i+"//"+config['CDMS_file1'],encoding='ISO-8859-1', sep="|")
        
        file2 = pd.read_csv(i+'//'+config['CDMS_file2'],encoding='ISO-8859-1', sep="|")
        
        file3 = pd.read_csv(i+"//"+config['CDMS_file3'],encoding='ISO-8859-1', sep="|")
        
        file4 = pd.read_csv(i+'//'+config['CDMS_file4'],encoding='ISO-8859-1', sep="|")
    
        headers = pd.read_csv('headers_matching.csv')
        
        headers = dict(list(zip(headers['key'],headers['value'])))
        
        file1.rename(columns = headers,inplace = True)
        
        file2.rename(columns = headers,inplace = True)
        
        file3.rename(columns = headers,inplace = True)
        
        file4.rename(columns = headers,inplace = True)
            
        
        
        #Merging all the files
        
        CDMS_merged = pd.merge(file1,file2,how = 'left',on = [config['customer_id']])
        
        CDMS_merged = pd.merge(CDMS_merged,file3,how = 'left',on = [config['customer_id']])
        
        CDMS_merged = pd.merge(CDMS_merged,file4,how = 'left',on = [config['customer_id']])
        
        #Columns rename
        
        # renaming_columns = dict(list(zip(config['columns_present'].split(','),config['columns_to_be_changed'].split(','))))
        
        # CDMS_merged.rename(columns = renaming_columns,inplace = True)
        
        print(i.replace(config['replace_string'],config['replace_with']+"//CDMS_output.csv"))
        
        #Creating Hash Code function
        
        def hash(sourcedf,destinationdf,column):
        
            columnName = 'hash_'
        
            for i in column:
        
                sourcedf[i] = sourcedf[i].fillna('')
        
                columnName = columnName + i
        
            hashColumn = pd.Series()
            
            for i in range((len(sourcedf[column[0]]))):
        
                concatstr = ''
        
                for j in column:
        
                    concatstr = concatstr + sourcedf[j][i]
        
                hashColumn.at[i] = hashlib.sha512( concatstr.encode("utf-8") ).hexdigest()
        
            destinationdf[columnName] = hashColumn
        
        CDMS_merged_hash1 = pd.DataFrame()
        
        CDMS_merged_hash2 = pd.DataFrame()
        
        hash(CDMS_merged,CDMS_merged_hash1,config['hash1_columns'].split(','))
        
        CDMS_output = pd.concat([CDMS_merged,CDMS_merged_hash1],axis = 1)
        
        CDMS_output.rename(columns = {'hash_'+''.join(config['hash1_columns'].split(',')):'hash1'},inplace = True)
        
        hash(CDMS_merged,CDMS_merged_hash2,config['hash2_columns'].split(','))
        
        CDMS_output = pd.concat([CDMS_output,CDMS_merged_hash2],axis = 1)
        
        CDMS_output.rename(columns = {'hash_'+''.join(config['hash2_columns'].split(',')):'hash2'},inplace = True)
        
        # CDMS_output = CDMS_output[config['output_columns'].split(',')]
        
        print(i.replace(config['replace_string'],config['replace_with'])+"//CDMS_output.csv")
        
        CDMS_output.to_csv(i.replace(config['replace_string'],config['replace_with'])+"//CDMS_output.csv",index = False)
        
        # body = {
    
        #     "fileName":"CDMS_output.csv",
    
        #     "filePath":i.replace(config['replace_string'],config['replace_with']),
    
        #     "subListID":141,
    
        #     "userID":"155",
    
        #     "businessHierarchyId":"29"
    
        # }
        
        
        # response = requests.post(url = 'MR402S0349D.palawangroup.com:8085/fileUploadExternalApi',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body)
        
        # upload_id = response.json()['content']['uploadId']
        
        # try:

        #     producer = KafkaProducer(bootstrap_servers='MR402S0352D.palawangroup:9092')

        #     topic = 'ftpKafkaConsumer'
         
        #     my_dict = {'fileUploadId': upload_id, 'filePath': i.replace(config['replace_string'],config['replace_with']), 'fileName': 'CDMS_value.csv'}

        #     my_dict = json.dumps(my_dict)

        #     producer.send(topic, value=my_dict.encode('utf-8'))

        #     print("Message sent successfully")
         
        # except Exception as e:

        #     print(f"Error: {e}")
        
        
        
        
        
        
