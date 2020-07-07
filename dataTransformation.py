import os
from pathlib import Path

import asyncio

DATALAKE_HOME_DIR = os.environ.get("DATALAKE_HOME_DIR", './raw')
SOURCE_DIRECTORY = Path(DATALAKE_HOME_DIR) 

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

def transformCsvFile(path, name, ext) :
    str = 'ls -l ' + path
    asyncio.run(command(str))


for item in SOURCE_DIRECTORY.iterdir():
    if item.is_dir() : continue # processing directory not allow yet
    
    abspath = os.path.abspath(item) # returns /home/user/file.ext
    filename =  item.stem # returns file
    fileext = item.suffix # returns .ext
    
    if fileext == '.csv' : transformCsvFile(abspath, filename, fileext)
    else : continue # file extension not allow yet
         


