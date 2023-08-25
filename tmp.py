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

# Nataional Address Database csv file and schema paths
srcFilePath     = Path(f'/home/kscott/apps/DataLakes/raw/NAD_r11.txt')
srcSchemaPath   = Path(f'/home/scott/apps/DataLakes/raw/NAD_schema.ini')

# Version of national address database collection/table
destVersion = "1.0" # Not in use today

# MongoDb destination collection name
destColName = 'nationaladdress'

# Actual size of file source file path
srcFilePathSize         = srcFilePath.stat().st_size
srcSchemaPathSize       = srcSchemaPath.stat().st_size

# Number of readlines batches inserts size
readlinesBulkWriteSize = 1000

# Calculate the size of each readlines before end of file (EOF)
readlinesHintSize = math.ceil(srcFilePathSize/(readlinesBulkWriteSize)) # use of actual available system RAM is better

def main():
    schema = loadNatlAddrSchemaData()
    #stageNatlAddrData(header)


def loadNatlAddrSchemaData() : []
""" Loads and returns an array map of National Address Schema data in squential order 
    loaded from schema file
    
    Schema data format
    [
        {
            field: column name
            type: column data type
            widht: length (optional)
        },
    ...]

"""
    print(f'INI Dump: National Address Database Schema. Source file {srcSchemaPath.name} size {srcSchemaPathSizePathSize}.\n')

    SCHEMA_DATA_FIELD_INDEX = 0 # *required
    SCHEMA_DATA_TYPE_INDEX  = 1 # *required
    SCHEMA_DATA_WIDTH_INDEX = 2 # optional
    SCHEMA_DATA_LEN_INDEX   = 3 # optional

    sf = open(file=srcSchemaPath.resolve(), mode='r', newline=None)
    lines = sf.readlines(hint=srcSchemaPathSize)

    schema_array = []

    # Ignores file descriptor lines
    for i in range(3, len(lines)) :
        schema = lines[i].split('=')[1] # column schema details: {field, type, width:optional, length:optional}}
        
        schema_data = {'field': schema[SCHEMA_DATA_FIELD_INDEX], 'type': schema[SCHEMA_DATA_TYPE_INDEX]}
        if len(schema) > 1 : 
            schema_data['width'] = schema[SCHEMA_DATA_LEN_INDEX]
        schema_array.append(schema_data)

    print(schema_array)
    return schema_array

def loadNatlAddrData(header) :
    print(f'CSV Dump: National Address Database. Source file {srcFilePath.name} size {srcFilePathSize}. Destination datalake.nataddr.csv.dump\n')
    
    # newline=None removes \n, \r or \n\r
    # results in actual file size difference reduction
    f = open(file=srcFilePath.resolve(), mode='r', newline=None)

    line = f.readline()
    header = line.split(',') # again, resuls in actual file size difference or reduction
        
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

        if len(json_array) > 0 :     
            collection.bulk_write(json_array)
        print(f'...DONE!')

    client.close()
    f.close()


if __name__ == "__main__" :
    main()
