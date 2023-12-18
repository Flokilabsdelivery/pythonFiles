# -*- coding: utf-8 -*-
"""
Created on Sat Dec 16 11:22:24 2023

@author: CBT
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Dec 14 01:01:00 2023

@author: CBT
"""

import warnings

import requests

import sys

# sys.path.append(r'D:/Python_exe/Lib\site-packages')

warnings.filterwarnings("ignore")

import hashlib

from unidecode import unidecode

import re

import pandas as pd

config = pd.read_excel('config.xlsx',engine = 'openpyxl')

config = dict(list(zip(config['key'],config['value'])))

business = pd.read_excel(config['business_configuration'],engine = 'openpyxl')

business.dropna(subset = ['File Name'],inplace = True)

business.dropna(subset = ['First Name'],inplace = True)

business.dropna(subset = ['Last Name'],inplace = True)


business.reset_index(inplace = True,drop = True)

business.fillna('',inplace = True)


# def hash(sourcedf,destinationdf,column):

#     columnName = 'hash_'

#     for i in column:

#         sourcedf[i] = sourcedf[i].fillna('')

#         columnName = columnName + i

#     hashColumn = pd.Series()
    
#     for i in range((len(sourcedf[column[0]]))):

#         concatstr = ''

#         for j in column:

#             concatstr = concatstr + sourcedf[j][i]

#         hashColumn.at[i] = hashlib.sha512( concatstr.encode("utf-8") ).hexdigest()

#     destinationdf[columnName] = hashColumn

def hash(row,column,hash_value):
    

    columnName = 'hash_'


    # hashColumn = pd.Series()
    
    # for i in range((len(sourcedf[column[0]]))):

    concatstr = ''

    for j in column:

        concatstr = concatstr + row[j]

    row[hash_value] = hashlib.sha512( concatstr.encode("utf-8") ).hexdigest()
    
    return row




def special_character_check_firstname(row):

    special_char=re.compile("//;,=?\[]\{}\(\)_`+&*'~#􀀀")

    if special_char.search(row['CustomerFirstName']) != None:
        
        if len(row['CustomerLastName'].split(' '))>1:
            
            temp_last_name = row['CustomerLastName'].split(' ')
            
            row['CustomerFirstName'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['CustomerLastName'] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            row['valid'] = 'invalid'

            if row['reason']=='':
            
                row['reason'] = 'special characters in First name'
                
            else:
                
                row['reason'] += ',Special characters in first name'
            
            
    return row

def special_character_check_lastname(row):

    special_char=re.compile("\/;,=?\[]\{}\(\)_`+&*'~#􀀀")
    
    if special_char.search(row['CustomerLastName']) != None:
        
        if len(row['CustomerFirstName'].split(' '))>1:
            
            temp_last_name = row['CustomerFirstName'].split(' ')
            
            row['CustomerFirstName'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['CustomerLastName'] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            row['valid'] = 'invalid'
            
            
            if row['reason']=='':
            
                row['reason'] = 'Special characters in last name'
                
            else:
                
                row['reason'] += ',Special characters in last name'

    return row



def number_check_firstname(row):
    
    if len(re.findall(r'[0-9]+', row['CustomerFirstName']))>0:
        
        
        if len(row['CustomerLastName'].split(' '))>1:
            
            
            temp_last_name = row['CustomerLastName'].split(' ')
            
            row['CustomerFirstName'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['CustomerLastName'] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            
            row['valid'] = 'invalid'

            if row['reason']=='':
            
                row['reason'] = 'Numeric characters in First name'
                
            else:
                
                row['reason'] += ',Numeric characters in first name'
            
            
    return row




def number_check_lastname(row):
    
    if len(re.findall(r'[0-9]+', row['CustomerLastName']))>0:
        
        if len(row['CustomerFirstName'].split(' '))>1:
            
            temp_last_name = row['CustomerFirstName'].split(' ')
            
            row['CustomerFirstName'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['CustomerLastName'] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            row['valid'] = 'invalid'
            
            
            if row['reason']=='':
            
                row['reason'] = 'Numeric characters in last name'
                
            else:
                
                row['reason'] += ',Numeric characters in last name'

    return row



def single_character_check_firstname(row):
    
    if len(row['CustomerFirstName'])<2:
        
        if len(row['CustomerLastName'].split(' '))>1:
            
            temp_last_name = row['CustomerLastName'].split(' ')
            
            row['CustomerFirstName'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['CustomerLastName'] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            row['valid'] = 'invalid'
            
            if row['CustomerFirstName']=='':
                    
                row['reason'] = 'Null Value in first name'

            else:
                
                row['reason'] = 'Single character in first name'
            
    return row

    
def single_character_check_lastname(row):
    
    if len(row['CustomerLastName'])<2:
        
        if len(row['CustomerFirstName'].split(' '))>1:
            
            temp_last_name = row['CustomerFirstName'].split(' ')
            
            row['CustomerFirstName'] = ' '.join(temp_last_name[0:len(temp_last_name)-1])
            
            row['CustomerLastName'] = temp_last_name[len(temp_last_name)-1]
        
        else:
            
            row['valid'] = 'invalid'
            
            if row['CustomerLastName']=='':
                
            
                if row['reason']=='':
                
                    row['reason'] = 'Null value in last name'
                    
                else:
                    
                    row['reason'] += ',Null value in last name'
            
            else:
                
                if row['reason']=='':
                
                    row['reason'] = 'Single character in last name'
                    
                else:
                    
                    row['reason'] += ',Single character in last name'

    return row
    

def change_accent(row):

    row['CustomerFirstName']= unidecode(row['CustomerFirstName'])

    row['CustomerLastName']= unidecode(row['CustomerLastName'])

    row['CustomerAddress1']= unidecode(row['CustomerAddress1'])

    return row

consolidated_status = business.copy()

consolidated_status['input_count'] = ''

consolidated_status['valid count'] = ''

consolidated_status['invalid_count'] = ''

consolidated_status['duplicates_in_input_count'] = ''

consolidated_status['duplicates_from_system'] = ''


for i in range(0,len(business)):
    
    print('processing '+business.loc[i,'File Name'])
    
    # print(business.loc[i,'File Name'])
    
    df = pd.read_csv(config['File_path']+"/"+business.loc[i,'File Name'],sep="|")
    
    consolidated_status.loc[i,'input_count'] = len(df)

    # df = pd.read_csv(config['File_path']+"\\"+"DEDUP_DOMESTIC_INWARD.csv",sep="|")

    
    columns = []
    
    for j in df.columns:
        
        columns.append(j.upper())
    
    df.columns = columns
    
    df.rename(columns = {business.loc[i,'First Name'].upper():config['FirstName']},inplace = True)    

    df.rename(columns = {business.loc[i,'Last Name'].upper():config['LastName']},inplace = True)    

    df.rename(columns = {business.loc[i,'Address'].upper():config['CustomerAddress']},inplace = True)    

    df.rename(columns = {business.loc[i,'DOB'].upper():config['CustomerDOB']},inplace = True)    

    missing_columns = set([config['FirstName'],config['LastName'],config['CustomerAddress'],config['CustomerDOB']]) - set(df.columns) 
    
    if len(missing_columns)>1:
        
        print()
        
        print(i)
        
        print('missing')
        
        print('columns missing in file '+business.loc[i,'File Name']+" : "+','.join(missing_columns))
        
        # continue

    #rule1 upper case and accent change



    df['CustomerFirstName'] = df['CustomerFirstName'].fillna('')   

    df['CustomerLastName'] = df['CustomerLastName'].fillna('')   

    df['CustomerAddress1'] = df['CustomerAddress1'].fillna('')   

 

    df['CustomerFirstName'] = df['CustomerFirstName'].str.upper()   

    df['CustomerLasttName'] = df['CustomerLastName'].str.upper()   

    df['CustomerAddress1'] = df['CustomerAddress1'].str.upper()   
        

    df = df.apply(lambda row:change_accent(row),axis = 1)
    
    #adding suffix

    suffixes = [ ' I', ' II', ' III', ' IV', ' V', ' JR', ' SR']    
    
    df['SUFFIX'] = ''
    
    for suffix in suffixes:
        
        df.loc[df['CustomerFirstName'].str.contains(suffix),'SUFFIX'] = suffix 

        df['CustomerFirstName'].str.replace(suffix,'')

        df.loc[df['CustomerLastName'].str.contains(suffix),'SUFFIX'] = suffix 

        df['CustomerLastName'].str.replace(suffix,'')

    # corporate customers
    
    corp_name_pattern = ['ACADEMY',"TECHNOLOGY" ,"TECHNO", "STALL","SERVICES","BRANCH","OUTLET","EXPRESS","CENTER","BUSINESS","CORPORATION","COMPANY"," INC","INC ","COURIER","COOPERATIVE","BANK","SECURITY","DISTRIBUTOR","DISTILLERS","PHARMACY","MOTORS","SCHOOL","TRADEING","ACCOUNTS","ASSOCIATION","UNIV","COLLEGES","MERCHANT/MERCHANDIZING","STORE","PHILS","INSTITUTE","LIMITED","ENTERPRISES","VENTURES",'SHOP', 'BOUTIQUE','CLINIC', 'HOSPITAL','FINANCIAL', 'PETROL', 'GASSTATION',"FUEL","DRUG","TRAVEL","TOURS",'TOURISM', 'RESTAURANT','LTD', 'FINANCE', 'REGION', 'MARKETING','DOLE NCR', 'FOOD', 'BAKERY',"CONSTRUCTION","BUILDERS","SUPPLY MATERIALS",'JEWELRY', 'JEWELERS', 'EDUCATIONAL',"AUTO",'MOTORCYCLE', 'PARTS' ,'INSURANCE',"HEALTH","WELLNESS","REALESTATE","PROPERTIES"]
    
    df['CORPORATE'] = False
    
    for corporate_key in corp_name_pattern:
        
        df_temp = df[df['CORPORATE']==False]
        
        df = df[df['CORPORATE']==True]
                
        df_temp['CORPORATE'] = df_temp['CustomerFirstName'].str.contains(corporate_key)
        
        df = pd.concat([df,df_temp],axis = 0)        
        
        df_temp = df[df['CORPORATE']==False]
        
        df = df[df['CORPORATE']==True]
        
        df_temp['CORPORATE'] = df_temp['CustomerLastName'].str.contains(corporate_key)
        
        df = pd.concat([df,df_temp],axis = 0)        
        
    df['valid'] = 'valid'
    
    df['reason'] = ''
        
    #single character



    
    # df['count'] = df['CustomerFirstName'].str.len()

    # df_temp = df[df['count']<2]
    
    # df = df[df['count']>=2]
    
    # temp_split = df_temp['CustomerLastName'].str.split(' ')
    
    # df_temp = pd.concat([df_temp,temp_split],axis = 1)
    
    print()
    
    print('single character check')


    df = df.apply(lambda row:single_character_check_firstname(row),axis = 1)

    df = df.apply(lambda row:single_character_check_lastname(row),axis = 1)
    
    print()
    
    print('numeric character check')
    
    df = df.apply(lambda row:number_check_firstname(row),axis = 1)

    df = df.apply(lambda row:number_check_lastname(row),axis = 1)

    print()    
    
    print('special character check')

    
    df = df.apply(lambda row:special_character_check_firstname(row),axis = 1)

    df = df.apply(lambda row:special_character_check_lastname(row),axis = 1)
    
    
    
    missing_columns = set(config['hash1_columns'].split(',')) - set(df.columns)
    
    for xy in missing_columns:
        
        df[xy] = ''

    missing_columns = set(config['hash2_columns'].split(',')) - set(df.columns)
    
    for xy in missing_columns:
        
        df[xy] = ''

    if 'CustomerBirthDate' in df.columns:
        
        df['CustomerBirthDate'] = df['CustomerBirthDate'].fillna('')
    
    
    df1 = pd.DataFrame()

    df2 = pd.DataFrame()
    
    
    print('Hash code generation started')

    df = df.apply(lambda row:hash(row,config['hash1_columns'].split(','),'hash1'),axis = 1)

    df = df.apply(lambda row:hash(row,config['hash2_columns'].split(','),'hash2'),axis = 1)

    # hash(df,df1,config['hash1_columns'].split(','))

    # hash(df,df2,config['hash2_columns'].split(','))

    # final_df = pd.concat([final_df,df2],axis = 1)

    # final_df.rename(columns = {'hash_'+''.join(config['hash2_columns'].split(',')):'hash2'},inplace = True)
    
    cdms = pd.read_csv('CDMS_merged.csv')
    
    
    final_df = df.copy()

    final_df_duplicates = final_df[final_df.duplicated(['hash1','hash2'])]

    final_df = final_df[~(final_df.duplicated(['hash1','hash2']))]

    final_df_duplicates['valid'] = 'invalid'
        
    # final_df_duplicates['reason'] +=',' 
    
    final_df_duplicates.loc[final_df_duplicates['reason']=='','reason'] = 'duplicates in raw data'
    
    final_df_duplicates.loc[final_df_duplicates['reason']!='','reason'] += 'duplicates in raw data'
    
    # final_df_duplicates.to_csv('invalid/duplicates_in_raw_data_'+business.loc[i,'File Name'],index = False)
    
    
    


    
    
    valid_output = final_df.copy()
    
    # duplicate_hash_df = pd.DataFrame()
    
    # duplicate_hash_list = []
    
    # for i in range(0,(len(df)//1000)):
        



    # for i in range(0,(len(df)//1000)):
        
        # hash_codes = final_df['hash2'][(i*1000):((i*1000)+1000)]

        # body = {"hashCodes":hash_codes}

        # response = requests.post(url = 'http://10.10.14.82:8011/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'6','isCustomer':True})

        # duplicate_hashcodes = response.json()        
        
        # duplicate_hash = final_df[final_df['hash2'].isin(duplicate_hashcodes)]

        # duplicate_hash_list.append(duplicate_hashcodes)

        
        # duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)
        
        # valid_output = valid_output[~(valid_output['hash2'].isin(duplicate_hashcodes))]
        
        

    # hash_codes = hash_codes[(len(df)//1000)*1000:]

    # body = {"hashCodes":hash_codes}

    # response = requests.post(url = 'http://10.10.14.82:8011/getCustomerDataby3Hashcode',headers = {'X-AUTH-TOKEN':'eyJ1c2VybmFtZSI6InN5c3RlbSIsInRva2VuIjoiODRjOWZmNmQtZTllMy00MWUwLWI0MDctZmY5ZGQ5YjFmYWU4In0=','Content-Type':'application/json'},json = body,params = {'BusinessId':'6','isCustomer':True})

    # duplicate_hashcodes = response.json()  

    # duplicate_hash_list.append(duplicate_hashcodes)  

    # cdms = pd.read_csv(r'C:\Users\CBT\Desktop\richard/CDMS_merged.csv')
    
    # duplicate_hash = final_df[final_df['hash2'].isin(duplicate_hash_list)]
    
    # # duplicate_hash_df = pd.concat([duplicate_hash_df,duplicate_hash],axis = 0)
    
    # # valid_output = valid_output[~(valid_output['hash2'].isin(duplicate_hashcodes))]
    
    
    # reading cdms and mcheck for duplicates









    # final_df = final_df[~final_df['hash2'].isin(duplicate_hash_list)]

    cdms = pd.read_csv('CDMS_merged.csv')

    hash1_duplicates = final_df[final_df['hash1'].isin(cdms['hash1'])]
    
    final_df = final_df[~(final_df['hash1'].isin(cdms['hash1']))]

    hash2_duplicates = final_df[final_df['hash2'].isin(cdms['hash2'])]
    
    final_df = final_df[~(final_df['hash2'].isin(cdms['hash2']))]

    duplicates = pd.concat([hash1_duplicates,hash2_duplicates],axis = 0)
    
    # duplicates.to_csv('invalid//duplicates_'+business.loc[i,'File Name'],index  = False)

    invalid_records = final_df[final_df['valid']=='invalid']
    
    final_df = final_df[final_df['valid']=='valid']    
        
    final_df.to_csv('valid/'+business.loc[i,'File Name'],index  = False)
    
    # duplicate_hash.to_csv('invalid//duplicates_'+business.loc[i,'File Name'],index  = False)
    
    final_df_duplicates['reason'] = 'duplicates in input'
    
    duplicates['reason'] = 'duplicates identified from system'

    invalid_records = pd.concat([invalid_records,final_df_duplicates],axis = 0)

    invalid_records = pd.concat([invalid_records,duplicates],axis = 0)
    
    invalid_records.to_csv('invalid//'+business.loc[i,'File Name'],index  = False)




    consolidated_status.loc[i,'valid count'] = len(final_df)
    
    consolidated_status.loc[i,'invalid count'] = len(invalid_records)
    
    consolidated_status.loc[i,'duplicates_in_input_count'] = len(final_df_duplicates)
    
    consolidated_status.loc[i,'duplicates_from_system'] = len(duplicates)
    
consolidated_status.to_csv('consolidation_count.csv',index = False)        
    
    
    







    