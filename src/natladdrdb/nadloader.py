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
    testLoadNatlAddrSchema()

    #stageNatlAddrData(header)

def testLoadNatlAddrSchema() :
    schema = loadNatlAddrSchemaData()
    print(f'testLoadNatlAddrSchema...', end='')

    if not schema == None : pass
    else : 
        print(f'NOT GOOD!\n\tloadNatlAddrSchemaData NONE!')
        return

    if schema.get("data_filename") == "[NAD_r11.txt]" : pass
    else : 
        print(f'NOT GOOD!\n\tdata_filename: {schema.get("data_filename")} not equal [NAD_r11.txt]')
        return

    if schema.get("format_type") == "CSVDelimited" : pass
    else : 
        print(f'NOT GOOD!\n\tformat_type: {schema.get("format_type")} not equal CSVDelimited')
        return

    if schema.get("headers_exists") == 'True' : pass
    else : 
        print(f'NOT GOOD!\n\theaders_exists: expects True not {schema.get("headers_exists")}')
        return

    if len(schema.get("headers").keys()) == 42 : pass
    else :
        print(f'NOT GOOD!\n\theaders count expects 42 not {len(schema.get("headers"))}')
        return

    data = schema.get("headers").get("OID")
    if not data == None and data["index"] == "1" and data["type"] == "Long" : pass
    else :
        print(f'NOT GOOD!\n\tOID column expects {{\'index\': \'1\', \'type\': \'Long\'}} not {data}')
        return

    data = schema.get("headers").get("State")
    if not data == None and data["index"] == "2" and data["type"] == "Text" and not data["width"] == None and  data["width"] == '2' : pass
    else :
        print(f'NOT GOOD!\n\tState column expects {{\'index\': \'2\', \'type\': \'Text\': \'2\'}} not {data}')
        return

    # example test case with a framework

    print(f'GOOD!')

def loadNatlAddrSchemaData() :     
    path  = Path(f'/home/kscott/apps/NAD/NAD_schema.ini')
    if not path.exists() or path.stat().st_size == 0 : return None # schema_ini
    
    print(f'INI Dump: National Address Database Schema. Access source file {path.name} with size {path.stat().st_size} memory load...', end='')
    
    SCHEMA_DATA_FIELD_INDEX = 0 # *required
    SCHEMA_DATA_TYPE_INDEX  = 1 # *required
    SCHEMA_DATA_WIDTH_INDEX = 2 # optional
    SCHEMA_DATA_LEN_INDEX   = 3 # optional

    # Load all schema data and close
    sfile= open(file=path.resolve(), mode='r', newline=None)
    lines = sfile.readlines(path.stat().st_size) 
    sfile.close()

    if not len(lines) == 45 : return None # schema_ini

    # format definition from June/July 2023
    schema = {
            'data_filename': lines[0].rstrip('\n'),                 # and not newline character
            'format_type': lines[1].split('=')[1].rstrip('\n'),     # and not new line character
            'headers_exists': lines[2].split('=')[1].rstrip('\n'),   # and not new line character
            'headers': {}                                           # initialize dictionary size empty or zero
    }

    # Ignores file descriptor lines
    for i in range(3, len(lines)) :
        data = lines[i].split('=') 
        
        # verify line format either
        #   COL1=field_name field_type {opptional: field_width field_length}
        if len(data) < 1 : continue # next loop. Line in wrong format 

        # save column header index
        index = data[0].lstrip('Col')

        data = data[1].rstrip('\n')     # remove any newline characters from strings end
        if len(data) == 0 : continue    # data does not exists

        schema_array = data.split(' ') # has single array [field_name, field_type, field_width:optional, field_length:optional]
        
        # create a new key value pair item
        key = schema_array[SCHEMA_DATA_FIELD_INDEX]
        schema['headers'][key] = {'index': index, 'type': schema_array[SCHEMA_DATA_TYPE_INDEX]}

        if len(schema_array) > 2 : # optional data items exists
            schema['headers'][key]['width'] = schema_array[SCHEMA_DATA_LEN_INDEX] # append the optional key value pair

    print(f'DONE!\n')
    return schema

def loadNatlAddrData(schema) :
    #format from June/July 2023
    path = Path(f'/home/kscott/apps/NAD/NAD_r11.txt')
    if not path.exists() or path.stat().st_size == 0: return

    print(f'CSV Dump: National Address Database. Source file {path.name} size {path.stat().st_size}. Destination datalake.nataddr.csv.dump\n')

    # Calculate the size of each readlines before end of file (EOF)
    readlinesHintSize = math.ceil(path.stat().st_size/(readlinesBulkWriteSize)) # use of actual available system RAM is better

    # results in actual file size difference reduction
    dfile = open(file=path.resolve(), mode='r', newline=None)

    headers = []
    if schema.get('has_headers') == 'True' :
        line = dfile.readline()
        headers = line.split(',').rstrip('\n') # again, resuls in actual file size difference or reduction
    
    client = MongoClient('mongodb://192.168.1.218:27017')
    database = client.get_database('datalake')
    collection = database.get_collection('natladdr.csv.dump')

    for index in range(readlinesBulkWriteSize) :
        if(dfile.closed) : break
        
        print(f'{index}. Bulk write started', sep='', end='')
        json_array = []
        for line in dfile.readlines(readlinesHintSize) :
            data = line.rstrip('\n').split(',') 
            data_exists = true

            # headers and data columns sequential array item index is the same
            if schema.get('headers_exists') == 'True' and len(headers)==len(data):
                data_exists = false
                headers = ''
                line = ''
                
            # zip() ignores any data array items, column, without a sequential header array item.
            json_data = {k:v for k,v in zip(header,data)} 
            json_array.append(InsertOne({
                'headers': headers,
                'data_exits': has_data,
                'data': line, # actual
                'json_data': json_data}))

        if len(json_array) > 0 :     
            collection.bulk_write(json_array)
        print(f'...DONE!')

    client.close()
    dfile.close()


if __name__ == "__main__" :
    main()
