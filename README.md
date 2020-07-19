
### ABSOLUTELY conscious that Microsoft SSIS Extract Transform Load ETL is similar
[Microsoft Azure DataLakes & Databricks](https://docs.microsoft.com/en-us/azure/databricks/scenarios/what-is-azure-databricks)  
[Amazon AWS Databricks](https://aws.amazon.com/marketplace/pp/Databricks-Inc-Databricks-Unified-Analytics-Platfo/B07K2NJKRW)

# Data Lake Development Project
A repository that helps me explore [Python](https://docs.python.org/3.7/index.html) [random](https://docs.python.org/3.7/library/random.html) module for generation of large development test data with specific function use. 

- randrange
- choice
- sample
  
[Visual Code](https://code.visualstudio.com/)  
[Python](https://www.python.org/)  
[MongoDB Ecosystem](https://docs.mongodb.com/ecosystem/drivers/)  
[GitHub](https://github.com/)  
[PyMongo](https://docs.mongodb.com/drivers/pymongo)  
[Docker-Py](https://docker-py.readthedocs.io/en/stable/)  
[Mongo Docker Hub](https://hub.docker.com/_/mongo)  
[Docker](https://www.docker.com/)  
[Robo3t](https://www.robomongo.org/)  
[Markdown Guide](https://guides.github.com/features/mastering-markdown/)  

## [Invoke](http://docs.pyinvoke.org/en/stable/getting-started.html)
Invoke is python task runner. It has similar functions found with yarn, npm or dotnet SDK project manager. Use requires tasks 
are defined with a default tasks.py or similar .py script files.

```
python -m pip invoke
python invoke --list
```

### [Build and Distribution Python](https://setuptools.readthedocs.io/en/latest/setuptools.html#developer-s-guide)
```
python setup.py --help
```

## NOTE: Dockerfile configuration manual at the moment
``` shell
docker run -p 27017:27017 --volume ~/apps/pyapps/datalake:/home/datalake --name mongo_dev mongo mongod
```

```
docker exec -it mongo_dev bash
```

```
mongoimport --db=datalake --collection=label_geography --type=csv --headerline --file= label_geography.csv
```