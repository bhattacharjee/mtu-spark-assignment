#!/bin/bash
#	
#-------------------------------------------------#
#						  #
# 1. Create the data server folders & processes	  #
#						  #
#-------------------------------------------------#
#	
# 1.1. Create the log directory
#	
mkdir logFiles
#	
# 1.2. Create the data directory for each of the replica sets servers
#	
mkdir node1
mkdir node5
mkdir node6
mkdir node2
mkdir node7
mkdir node8
mkdir node3
mkdir node9
mkdir node10
mkdir node4
mkdir node11
mkdir node12
#	
# 1.3. Start each member of the replica set 
#	
mongod --shardsvr --replSet replset1 --dbpath node1 --port 27000 --logpath logFiles/node1.log --fork >> logFiles/log.file
mongod --shardsvr --replSet replset1 --dbpath node5 --port 27001 --logpath logFiles/node5.log --fork >> logFiles/log.file
mongod --shardsvr --replSet replset1 --dbpath node6 --port 27002 --logpath logFiles/node6.log --fork >> logFiles/log.file
mongod --shardsvr --replSet replset2 --dbpath node2 --port 27100 --logpath logFiles/node2.log --fork >> logFiles/log.file
mongod --shardsvr --replSet replset2 --dbpath node7 --port 27101 --logpath logFiles/node7.log --fork >> logFiles/log.file
mongod --shardsvr --replSet replset2 --dbpath node8 --port 27102 --logpath logFiles/node8.log --fork >> logFiles/log.file
mongod --shardsvr --replSet replset3 --dbpath node3 --logpath logFiles/node3.log --port 27200 --fork >> logFiles/log.file
mongod --shardsvr --replSet replset3 --dbpath node9 --logpath logFiles/node9.log --port 27201 --fork >> logFiles/log.file
mongod --shardsvr --replSet replset3 --dbpath node10 --logpath logFiles/node10.log --port 27202 --fork >> logFiles/log.file
mongod --shardsvr --replSet replset4 --dbpath node4 --logpath logFiles/node4.log --port 27300 --fork >> logFiles/log.file
mongod --shardsvr --replSet replset4 --dbpath node11 --logpath logFiles/node11.log --port 27301 --fork >> logFiles/log.file
mongod --shardsvr --replSet replset4 --dbpath node12 --logpath logFiles/node12.log --port 27302 --fork >> logFiles/log.file
#	
#-------------------------------------------------#
#						  #
# 2. Create the config server folders & processes #
#						  #
#-------------------------------------------------#
#	
# 2.1. Create the data directory for each of the config servers
#	
mkdir node13
mkdir node14
mkdir node15
#	
# 2.2. Start the config server instances 
#	
mongod --configsvr --replSet metadata --dbpath node13 --port 26050 --logpath logFiles/node13.log --fork >> logFiles/log.file
mongod --configsvr --replSet metadata --dbpath node14 --port 26051 --logpath logFiles/node14.log --fork >> logFiles/log.file
mongod --configsvr --replSet metadata --dbpath node15 --port 26052 --logpath logFiles/node15.log --fork >> logFiles/log.file
#	
# 2.3. Wait for 20 seconds before continuing
#	
sleep 20s
#
#---------------------------------------------------#
#						    #
# 3. Set up data and config servers as replica sets #
#						    #
#---------------------------------------------------#
#	
mongo --port 26050 --shell 1.replica_sets.js
# 	
#-------------------------------------------------#
#						  #
# 4. Start the mongos instances   		  #
#						  #
#-------------------------------------------------#
#	
# 4.1. A first mongos process listens to the default port 27017
#	
mongos --configdb metadata/localhost:26050,localhost:26051,localhost:26052 --logpath logFiles/mongos0.log --fork >> logFiles/log.file
#	
# 4.2. Remaining mongos processes listen to the explicit ports assigned by us
#	
mongos --configdb metadata/localhost:26050,localhost:26051,localhost:26052 --port 26061 --logpath logFiles/mongos1.log --fork >> logFiles/log.file
mongos --configdb metadata/localhost:26050,localhost:26051,localhost:26052 --port 26062 --logpath logFiles/mongos2.log --fork >> logFiles/log.file
mongos --configdb metadata/localhost:26050,localhost:26051,localhost:26052 --port 26063 --logpath logFiles/mongos3.log --fork >> logFiles/log.file
#	
# 4.3. Wait for 10 seconds before continuing
#	
sleep 10s
#	
#---------------------------------------------------#
#						    #
# 5. Set up the Shards				    #
#						    #
#---------------------------------------------------#
#	
mongo --shell 2.shards.js
#	
#---------------------------------------------------#
#						    #
# 6. Add the collection				    #
#						    #
#---------------------------------------------------#
#	
# 6.1. We insert the collection from the dataset file
#
mongoimport --db my_database --collection restaurants --drop --file dataset.json
#	
# 6.2. Wait for 10 seconds before continuing
#	
sleep 10s
#	
#---------------------------------------------------#
#						    #
# 7. Shard the collection			    #
#						    #
#---------------------------------------------------#
#	
mongo --shell 3.shard_collection.js
# 


