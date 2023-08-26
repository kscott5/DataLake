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

# Version of national address database collection/table
destVersion = "1.0" # Not in use today

# MongoDb destination collection name
destColName = 'nationaladdress'

# Number of readlines batches inserts size
readlinesBulkWriteSize = 1000


def main():
    schema = loadNatlAddrSchemaData()
    #stageNatlAddrData(header)


def loadNatlAddrSchemaData() :     
    path  = Path(f'/home/kscott/apps/DataLakes/raw/NAD_schema.ini')
    if not path.exists() or path.stat().st_size == 0 : return None # schema_ini
    
    print(f'INI Dump: National Address Database Schema. Source file {path.name} size {path.stat().st_size}.\n')
    
    SCHEMA_DATA_FIELD_INDEX = 0 # *required
    SCHEMA_DATA_TYPE_INDEX  = 1 # *required
    SCHEMA_DATA_WIDTH_INDEX = 2 # optional
    SCHEMA_DATA_LEN_INDEX   = 3 # optional

    # Load all schema data and close
    sfile= open(file=path.resolve(), mode='r', newline=None)
    lines = sfile.readlines(path.stat().st_size) 
    sfile.close()

    if not len(lines) == 45 : return None # schema_ini

    schema_ini = {
            'data_filename': line[0],
            'format_type': line[1],
            'header_exists': line[2],
            'header_schema_array': []
    }

    # Ignores file descriptor lines
    for i in range(3, len(lines)) :
        data = lines[i].split('=') 
        
        # verify line format either
        #   COL1=field_name field_type {opptional: field_width field_length}
        if len(data) < 1 : continue # next loop. Line in wrong format 

        # ignore first item at array index 0
        data = data[1].rstrip('\n')     # remove any newline characters from strings end
        if len(data) == 0 : continue    # data does not exists

        schema = data.split(' ') # has single array [field_name, field_type, field_width:optional, field_length:optional]
        
        # create a new key value pair item
        schema_data = {'field': schema[SCHEMA_DATA_FIELD_INDEX], 'type': schema[SCHEMA_DATA_TYPE_INDEX]}

        if len(schema) > 2 : # optional data items exists
            schema_data['width'] = schema[SCHEMA_DATA_LEN_INDEX] # append the optional key value pair

        # append new column key value pair schema data at end of array
        schema_ini['header_schema_array'].append(schema_data)

    print(schema_ini)
    return schema_ini

def loadNatlAddrData(header) :
    path = Path(f'/home/kscott/apps/DataLakes/raw/NAD_r11.txt')
    if not path.exists() or path.stat().st_size == 0: return

    print(f'CSV Dump: National Address Database. Source file {path.name} size {path.stat().st_size}. Destination datalake.nataddr.csv.dump\n')

    # Calculate the size of each readlines before end of file (EOF)
    readlinesHintSize = math.ceil(path.stat().st_size/(readlinesBulkWriteSize)) # use of actual available system RAM is better

    # results in actual file size difference reduction
    dfile = open(file=path.resolve(), mode='r', newline=None)

    line = dfile.readline()
    header = line.split(',').rstrip('\n') # again, resuls in actual file size difference or reduction
        
    client = MongoClient('mongodb://localhost:27017')
    database = client.get_database('datalake')
    collection = database.get_collection('nataddr.csv.dump')

    for index in range(readlinesBulkWriteSize) :
        if(dfile.closed) : break
        
        print(f'{index}. Bulk write started', sep='', end='')
        json_array = []
        for line in dfile.readlines(readlinesHintSize) :
            data = line.split(',') 

            json_data = {}
            if len(header) == len(data) :        
                json_data = {k:v for k,v in zip(header,data)}

            json_array.append(InsertOne({'has_json': len(header)==len(data), 'header': header, 'raw_data': line, 'json_data': json_data}))

        if len(json_array) > 0 :     
            collection.bulk_write(json_array)
        print(f'...DONE!')

    client.close()
    dfile.close()


if __name__ == "__main__" :
    main()
