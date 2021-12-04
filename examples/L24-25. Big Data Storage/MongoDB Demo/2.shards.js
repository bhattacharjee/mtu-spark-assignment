//----------------------------------------------------
//
//  5. Set up the Shards
//
//----------------------------------------------------
//
//------------------------------------------------
// 5.1. Variables
//------------------------------------------------
//
db = db.getSisterDB("config");
var mongosConn = db;
var res = null;
//
//------------------------------------------------
// 7.2. Set the chunk size
//------------------------------------------------
//
db.settings.save( { _id:"chunksize", value: 1 } )
//
//------------------------------------------------
// 5.2. Shard_1
//------------------------------------------------
//
// 5.2.1. Connect to mongod
//
db = connect("localhost:27000/my_database");
//
// 5.2.2. Add the shard
//
db = mongosConn;
res = sh.addShard("replset1/localhost:27000");
while (res.ok != 1){
    sleep(60);
    if (res.ok != 1){
        print("Adding Shard Failed. Trying it again");
        res = sh.addShard("replset1/localhost:27000");
    }
}
print("Shard_1 Added!");
//
//------------------------------------------------
// 5.3. Shard_2
//------------------------------------------------
//
// 5.3.1. Connect to mongod
//
db = connect("localhost:27100/my_database");
//
// 5.3.2. Add the shard
//
db = mongosConn;
res = sh.addShard("replset2/localhost:27100");
while (res.ok != 1){
    sleep(60);
    if (res.ok != 1){
        print("Adding Shard Failed. Trying it again");
        res = sh.addShard("replset2/localhost:27100");
    }
}
print("Shard_2 Added!");
//
//------------------------------------------------
// 5.4. Shard_3
//------------------------------------------------
//
// 5.4.1. Connect to mongod
//
db = connect("localhost:27200/my_database");
//
// 5.4.2. Add the shard
//
db = mongosConn;
res = sh.addShard("replset3/localhost:27200");
while (res.ok != 1){
    sleep(60);
    if (res.ok != 1){
        print("Adding Shard Failed. Trying it again");
        res = sh.addShard("replset3/localhost:27200");
    }
}
print("Shard_3 Added!");
//
//------------------------------------------------
// 5.5. Shard_4
//------------------------------------------------
//
// 5.5.1. Connect to mongod
//
db = connect("localhost:27300/my_database");
//
// 5.5.2. Add the shard
//
db = mongosConn;
res = sh.addShard("replset4/localhost:27300");
while (res.ok != 1){
    sleep(60);
    if (res.ok != 1){
        print("Adding Shard Failed. Trying it again");
        res = sh.addShard("replset4/localhost:27300");
    }
}
print("Shard_4 Added!");
//
//------------------------------------------------
// 5.6. Quit
//------------------------------------------------
//
quit()
