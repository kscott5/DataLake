import os
import re
import asyncio
import sched
import time
from pathlib import Path

# Environment variables
MONGO_DB_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
DATALAKE_DB_NAME = os.environ.get('DATALAKE_DB_NAME', 'datalake')
DATALAKE_HOME_DIR = os.environ.get('DATALAKE_HOME_DIR', './raw')
SOURCE_DIRECTORY = Path(DATALAKE_HOME_DIR) 

MONGO_IMPORT_LINE = 'mongoimport  --uri={0}/{1} --collection={2} --type=csv --headerline --file={3}'

#Event scheduler
eventScheduler = sched.scheduler(time.time, time.sleep)

# 
#
#
async def command(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    print(f'[{cmd!r} exited with {proc.returncode}]')
    if stdout:
        print(f'[stdout]\n{stdout.decode()}')
    if stderr:
        print(f'[stderr]\n{stderr.decode()}')

# Imports data from CSV file with mongoimports
# #
# path: absolute path with file and extension
# name: csv file name
# ext:  extention of file
#
# https://docs.mongodb.com/manual/reference/program/mongoimport/
def transformCsvFile(path, file, extension) :
    collection = re.sub(extension, '', file) # collection name
    line = MONGO_IMPORT_LINE.format(MONGO_DB_URI, DATALAKE_DB_NAME, collection, path)

    asyncio.run(command(line))

def transformRawDataFiles():
    for item in SOURCE_DIRECTORY.iterdir():
        if item.is_dir() : continue # processing directory not allow yet
        
        abspath = os.path.abspath(item) # returns /home/user/file.ext
        filename =  item.stem # returns file
        fileext = item.suffix # returns .ext
        
        if fileext == '.csv' : transformCsvFile(abspath, filename, fileext)
        else : continue # file extension not allow yet
         
# https://docs.python.org/3.7/library/__main__.html?highlight=__main__
def main():
    print("Main event scheduler for Data Transformation")
    eventScheduler.enter(300, 1, transformRawDataFiles)
    eventScheduler.run(False)
    
if __name__ == "__main__":
    main()