import io
import math
from pathlib import Path

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.operations import InsertOne

# NOTE: Standard random access memory (RAM) on smart devices 
#       in year 2023 is gigabytes
kilobyte = 1024             # 1 Kilobyte in bytes
megabyte = kilobyte*1000    # 1 Megabbyte in kilobytes
gigabyte = megabyte*1000    # 1 Gigabyte in megabytes

# Nataional Address Database csv file path
srcFilePath = Path('/home/kscott/apps/DataLakes/raw/NAD_r11.txt')

# MongoDb destination collection name
destColName = 'nationaladdress'

# Actual size of file source file path
srcFilePathSize = srcFilePath.stat().st_size

# Number of readlines batches inserts size
readlinesBulkWriteSize = 1000

# Calculate the size of each readlines before end of file (EOF)
readlinesHintSize = math.ceil(srcFilePathSize/(readlinesBulkWriteSize)) # use of actual available system RAM is better

def main():
    f = open(file=srcFilePath.resolve(), mode='r', newline=None)

    line = f.readline()
    header = line.split(',')
    stageNatlAddrData(f,header)

    f.close()

def stageNatlAddrData(f,header) :
    print(f'CSV Dump: National Address Database. Source file {srcFilePath.name} size {srcFilePathSize}. Destination datalake.nataddr.csv.dump\n')
    
    client = MongoClient('mongodb://localhost:27017')
    database = client.get_database('datalake')
    collection = database.get_collection('nataddr.csv.dump')

    for index in range(readlinesBulkWriteSize) :
        if(f.closed) : break
        
        print(f'{index}. Bulk write started', sep='', end='')
        json_array = []
        for line in f.readlines(readlinesHintSize) :
            data = line.split(',') 

            json_data = {}
            if len(header) == len(data) :        
                json_data = {k:v for k,v in zip(header,data)}

            json_array.append(InsertOne({'has_json': len(header)==len(data), 'header': header, 'raw_data': line, 'json_data': json_data}))

        collection.bulk_write(json_array)
        print(f'...DONE!')

    client.close()


if __name__ == "__main__" :
    main()
