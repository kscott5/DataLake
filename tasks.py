from invoke import task, run
from pathlib import Path

@task
def clean(context):
    path = Path('./dist')
    if path.exists() and path.is_dir() :
        run('rm -rf ./dist')
    
    path = Path('./build')
    if path.exists() and path.is_dir() :
        run('rm -rf ./build')

@task(pre=[clean])
def build(context):
    run('python setup.py build')
    run('mkdir ./dist')
