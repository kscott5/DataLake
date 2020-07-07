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


### ABSOLUTELY conscious that Microsoft SSIS Extract Transform Load ETL is similar
[Microsoft Azure DataLakes & Databricks](https://docs.microsoft.com/en-us/azure/databricks/scenarios/what-is-azure-databricks)  
[Amazon AWS Databricks](https://aws.amazon.com/marketplace/pp/Databricks-Inc-Databricks-Unified-Analytics-Platfo/B07K2NJKRW)
