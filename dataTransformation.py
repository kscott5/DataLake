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

#Event scheduler
eventScheduler = sched.scheduler(time.time, time.sleep)

# 
#
#
async def command(name, action):
    print(f'Command: {name}action: {action}')
    
    proc = await asyncio.create_subprocess_shell(
        action,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    results = {'isOk': proc.returncode == 0, 'code': '', 'message': '' }
    results['code'] = f'[{name!r} exited with {proc.returncode}]'
    print(f"{results['code']}")

    if stdout:
        results['message'] = f'[stdout]\n{stdout.decode()}'
    elif stderr:
        results['message'] = f'[stderr]\n{stderr.decode()}'
    
    return results

# Imports data from CSV file with mongoimports
# #
# path: absolute path with file and extension
# name: csv file name
# ext:  extention of file
#
# https://docs.mongodb.com/manual/reference/program/mongoimport/
async def transformCsvFile(path, file, extension) :
    print(f'Start: transformCsvFile({file})')
    collection = re.sub(extension, '', file) # collection name

    # https://docs.python.org/3.7/tutorial/inputoutput.html
    line = f'mongoimport  --uri={MONGO_DB_URI}/{DATALAKE_DB_NAME} --collection={collection} --type=csv --headerline --file={path}'    
    results = await command('mongoimport', line)    

    if results['isOk'] :
        Path(path).rename(path.replace(extension, f'{extension}.{time.time()}'))

def transformRawDataFiles():
    print('Start: transformRawDataFiles()')
    for item in SOURCE_DIRECTORY.iterdir():
        if item.is_dir() : continue # processing directory not allow yet
        
        abspath = os.path.abspath(item) # returns /home/user/file.ext
        filename =  item.stem # returns file
        fileext = item.suffix # returns .ext
        
        if fileext == '.csv' : asyncio.run(transformCsvFile(abspath, filename, fileext))
        else : continue # file extension not allow yet
    
    print('Done')

# https://docs.python.org/3.7/library/__main__.html?highlight=__main__
def main():
    print('Main event scheduler for Data Transformation')

    eventScheduler.enter(60, 1, transformRawDataFiles)
    eventScheduler.run(True)

    print('Main event scheduler for Data Transformation complete for now')
    
if __name__ == "__main__":
    main()