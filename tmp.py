from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.operations import InsertOne

srcFilePath = '/home/kscott/apps/DataLakes/raw/NAD_r11.txt'
destColName = 'nationaladdress'

f = open(file=srcFilePath, mode='r', newline='\n')

line = f.readline()
header = line.split(',')

client = MongoClient('mongodb://localhost:27017')
database = client.get_database('datalake')
collection = database.get_collection('nataddr.csv.dump')

json_array = []
for line in f.readlines(10000) :
    data = line.split(',') 

    json_data = {}
    if len(header) == len(data) :        
        json_data = {k:v for k,v in zip(header,data)}

    json_array.append(InsertOne({'has_json': len(header)==len(data), 'header': header, 'raw_data': line, 'json_data': json_data}))    

collection.bulk_write(json_array)

client.close()
f.close()
