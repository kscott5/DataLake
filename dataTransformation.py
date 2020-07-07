import os
import re
import asyncio
from pathlib import Path

DATALAKE_DB_NAME = os.environ.get('DATALAKE_DB_NAME', 'datalake')
DATALAKE_HOME_DIR = os.environ.get('DATALAKE_HOME_DIR', './raw')
SOURCE_DIRECTORY = Path(DATALAKE_HOME_DIR) 

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
    line = format('mongoimport --db=%s --collection=%s --type=csv --headerline --file=%s', 
        DATALAKE_DB_NAME, collection, path)

    asyncio.run(command(line))

for item in SOURCE_DIRECTORY.iterdir():
    if item.is_dir() : continue # processing directory not allow yet
    
    abspath = os.path.abspath(item) # returns /home/user/file.ext
    filename =  item.stem # returns file
    fileext = item.suffix # returns .ext
    
    if fileext == '.csv' : transformCsvFile(abspath, filename, fileext)
    else : continue # file extension not allow yet
         


