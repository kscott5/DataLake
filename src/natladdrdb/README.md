
# Rescue Shelter and MongoDB Sharding
A repository that helps me explore [MongoDB Shard Cluster](https://docs.mongodb.com/manual/tutorial/deploy-shard-cluster/) and [Python](https://docs.python.org/3.7/index.html) [random](https://docs.python.org/3.7/library/random.html) module for generation of large development test data with specific function use.

## [Deploy Shard Clusters Workflow](https://docs.mongodb.com/manual/tutorial/deploy-shard-cluster/)
The instruction for shard deployment are at [https://docs.mongodb.com/manual/tutorial/deploy-shard-cluster/](https://docs.mongodb.com/manual/tutorial/deploy-shard-cluster/).   

```
* Read the instruction slow.
* mongod  --replSet [name] option are different
* mongod  --port    [0000] option overrides ANY default
```

#### Examples
```
mongod   --configsvr --replSet confg27020 --port 27020...
mongod   --shardsvr  --replSet shard27021 --port 27021...
mongod   --shardsvr  --replSet shard27022 --port 27022...
mongod   --shardsvr  --replSet shard27023 --port 27023...
```

### NOTE: [RescueShelter.Reports](https://github.com/kscott5/RescueShelter.Reports/blob/master/README.md) 
This deployment workflow expects the mongo docker images was already pull from a public or private repository.

## MongoDB folder system structure for Rescue Shelter data
```
mkdir /data/rescueshelter/db
mkdir /data/rescueshelter/confg27020
mkdir /data/rescueshelter/shard27021
mkdir /data/rescueshelter/shard27022
mkdir /data/rescueshelter/shard27023
```