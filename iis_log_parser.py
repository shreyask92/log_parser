# -*- coding: utf-8 -*-
"""
Author: Shreyas Krishnamurthy
"""
import os
import datetime as dt
import unicodedata
import pymongo
import re


## Function to check if string is integer
def is_number(s):
    try:
        int(s)
        return int(s)
    except ValueError:
        pass
 
    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False


def read_log(fileName,fileDir):
    # The header variable stores the fields in the log files
    header = []
    # a list to save the dictionary for each line in the IIS log file
    l = []
    print('Processing the file '+fileName)
    ## try..except block to ensure that we do not encounter encoding errors
    try:
        file_data = open(fileDir+fileName,encoding="utf8").readlines()
    except:
        file_data = open(fileDir+fileName).readlines()
    
    ## reading and parsing the lines in the log file
    for line in file_data:
        if not line.startswith('#'):
            fields = line.split()
            date_idx = -1
            time_idx = -1
            for idx,val in enumerate(fields):
                if re.match('20[0-9][0-9]-[0-1][0-9]-[0-3][0-9]', val):
                    date_idx = idx
                elif re.match('[0-2][0-9]:[0-5][0-9]:[0-5][0-9]', val):
                    time_idx = idx
                elif is_number(val):
                    fields[idx] = is_number(val)
            fields[date_idx]=dt.datetime.strptime(fields[date_idx]+' '+fields[time_idx],'%Y-%m-%d %H:%M:%S')
            fields.pop(time_idx)
            #pprint (fields) # debug
            d = dict(zip(header, fields)) # create a <dict> based on <headers> & <split> log lines
            l.append(d)
        elif line.split()[0] == '#Fields:':
            header = line.split()
            header.remove('#Fields:')
            header.remove('time')
    return l


## function to insert the log data into the the database collection 
## Modify this function based on your requirements
def insertMongoDB():
    ## connection to mongodb
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    ## creating the database
    mydb = myclient["database_name"]
    ## creating a collection in the db
    mylogs = mydb["collection_name"]
    
    ## dir with all the log files
    log_dir = "/Data/"

    # ## reading all the log files in the dir, which have to be parsed and inserted into the DB
    # allFiles = os.listdir(log_dir)
    # for file_name in allFiles:
    #     l = read_log(file_name,log_dir)
    #     mylogs.insert_many(l)
    
    ## only for one log file
    l = read_log('iislog_sample.log',log_dir)
    mylogs.insert_many(l)
          
    print(myclient.list_database_names())
    print(mydb.list_collection_names())

    