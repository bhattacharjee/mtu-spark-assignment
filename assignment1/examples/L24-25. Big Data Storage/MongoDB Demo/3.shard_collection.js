//----------------------------------------------------
//
//  7. Shard the collection
//
//----------------------------------------------------
//
//------------------------------------------------
// 7.1. Variables
//------------------------------------------------
//
db = db.getSisterDB("my_database");
var res = null;
//
//------------------------------------------------
// 7.2. Set the chunk size
//------------------------------------------------
//
db.settings.save( { _id:"chunksize", value: 1 } )
//
//------------------------------------------------
// 7.3. Enable the test database for sharding
//------------------------------------------------
//
res = sh.enableSharding("my_database");
while (res.ok != 1) {
    sleep(10);
    if (res.ok != 1){
        print("Enable my_database for Sharding Failed. Trying it again");
        res = sh.enableSharding("my_database");
    }
}
print("my_database Enable for Sharding!");
//
//------------------------------------------------
// 7.4. Create an index for the collection
//------------------------------------------------
//
res = db.restaurants.createIndex({ "cuisine" : 1, "borough" : 1 });
while (res.ok != 1) {
    sleep(10);
    if (res.ok != 1){
        print("Creating index for restaurants collection failed. Trying it again");
        res = db.restaurants.createIndex({ "cuisine" : 1, "borough" : 1 });
    }
}
print("restaurant Collection Index Created!");
//
//------------------------------------------------
// 7.5. Shard the collection by this index
//------------------------------------------------
//
res = sh.shardCollection("my_database.restaurants", { "cuisine" : 1, "borough" : 1 } );
while (res.ok != 1) {
    sleep(10);
    if (res.ok != 1){
        print("Sharding restaurants collection failed. Trying it again");
        res = sh.shardCollection("test.restaurants", { "cuisine" : 1, "borough" : 1 } );
    }
}
print("restaurant Collection Sharded!");
//
//------------------------------------------------
// 7.6. Print the sharding evolution
//------------------------------------------------
//
for (i = 0; i < 10; i++) {
    sh.status();
    sleep(10000);
}
//
//------------------------------------------------
// 7.7. Quit
//------------------------------------------------
//
quit()
