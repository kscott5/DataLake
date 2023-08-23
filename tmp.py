import io
import os
from pathlib import Path

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.operations import InsertOne

# Nataional Address Database csv file path
srcFilePath = os.path('/home/kscott/apps/DataLakes/raw/NAD_r11.txt')

# MongoDb destination collection name
destColName = 'nationaladdress'

# Actual size of file source file path
srcFilePathSize = srcFilePath.stat().st_size
print(f'')

def main():
    f = open(file=srcFilePath.resolve(), mode='r', newline='\n')

    line = f.readline()
    header = line.split(',')
    stageNatlAddrData(f,header)

    f.close()

def stageNatlAddrData(f,header) :
    print(f'starting stage national address data function')
    if f.closed  : 
        print('closed')
        return

    client = MongoClient('mongodb://localhost:27017')
    database = client.get_database('datalake')
    collection = database.get_collection('nataddr.csv.dump')

    print('mongo')
    json_array = []
    for line in f.readlines(10000) :
        data = line.split(',') 

        json_data = {}
        if len(header) == len(data) :        
            json_data = {k:v for k,v in zip(header,data)}

        json_array.append(InsertOne({'has_json': len(header)==len(data), 'header': header, 'raw_data': line, 'json_data': json_data}))

    collection.bulk_write(json_array)
    client.close()

    stageNatlAddrData(f,header)

if __name__ == "__main__" :
    main()
