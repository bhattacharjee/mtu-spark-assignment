//----------------------------------------------------
//
//  3. Set up data and config servers as replica sets
//
//----------------------------------------------------
//
//------------------------------------------------
// 3.1. Variables
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
// 3.2. Metadata Replica Set
//------------------------------------------------
//
// 3.2.1. Connect to mongod
//
db = connect("localhost:26050/my_database");
//
// 3.2.2. Initiate the replica set
//
res = rs.initiate(
    {
        "_id" : "metadata",
        "members" : [
            { _id:0,host:"localhost:"+"26050" },
            { _id:1,host:"localhost:"+"26051" },
            { _id:2,host:"localhost:"+"26052" }
        ]
    }
);
//
// 3.2.3. Wait until all nodes of the replica set are up and running
//
while (res.ok != 1){
    sleep(10);
}
print("Replica Set metadata Created!");
while (((rs.status().members[0].state != 1) && (rs.status().members[0].state != 2)) || ((rs.status().members[1].state != 1) && (rs.status().members[1].state != 2)) || ((rs.status().members[2].state != 1) && (rs.status().members[2].state != 2))) {
    sleep(10);
}
print("Replica Set metadata Up!");
//
//------------------------------------------------
// 3.3. replset1
//------------------------------------------------
//
// 3.3.1. Connect to mongod
//
db = connect("localhost:27000/my_database");
//
// 3.3.2. Initiate the replica set
//
res = rs.initiate(
    {
        "_id" : "replset1",
        "members" : [
            { _id:0,host:"localhost:"+"27000" },
            { _id:1,host:"localhost:"+"27001" },
            { _id:2,host:"localhost:"+"27002" }
        ]
    }
);
//
// 3.3.3. Wait until all nodes of the replica set are up and running
//
while (res.ok != 1){
    sleep(10);
}
print("Replica Set replset1 Created!");
while (((rs.status().members[0].state != 1) && (rs.status().members[0].state != 2)) || ((rs.status().members[1].state != 1) && (rs.status().members[1].state != 2)) || ((rs.status().members[2].state != 1) && (rs.status().members[2].state != 2))) {
    sleep(10);
}
print("Replica Set replset1 Up!");
//
//------------------------------------------------
// 3.4. replset2
//------------------------------------------------
//
// 3.4.1. Connect to mongod
//
db = connect("localhost:27100/my_database");
//
// 3.4.2. Initiate the replica set
//
res = rs.initiate(
    {
        "_id" : "replset2",
        "members" : [
            { _id:0,host:"localhost:"+"27100" },
            { _id:1,host:"localhost:"+"27101" },
            { _id:2,host:"localhost:"+"27102" }
        ]
    }
);
//
// 3.4.3. Wait until all nodes of the replica set are up and running
//
while (res.ok != 1){
    sleep(10);
}
print("Replica Set replset2 Created!");
while (((rs.status().members[0].state != 1) && (rs.status().members[0].state != 2)) || ((rs.status().members[1].state != 1) && (rs.status().members[1].state != 2)) || ((rs.status().members[2].state != 1) && (rs.status().members[2].state != 2))) {
    sleep(10);
}
print("Replica Set replset2 Up!");
//
//------------------------------------------------
// 3.5. replset3
//------------------------------------------------
//
// 3.5.1. Connect to mongod
//
db = connect("localhost:27200/my_database");
//
// 3.5.2. Initiate the replica set
//
res = rs.initiate(
    {
        "_id" : "replset3",
        "members" : [
            { _id:0,host:"localhost:"+"27200" },
            { _id:1,host:"localhost:"+"27201" },
            { _id:2,host:"localhost:"+"27202" }
        ]
    }
);
//
// 3.5.3. Wait until all nodes of the replica set are up and running
//
while (res.ok != 1){
    sleep(10);
}
print("Replica Set replset3 Created!");
while (((rs.status().members[0].state != 1) && (rs.status().members[0].state != 2)) || ((rs.status().members[1].state != 1) && (rs.status().members[1].state != 2)) || ((rs.status().members[2].state != 1) && (rs.status().members[2].state != 2))) {
    sleep(10);
}
print("Replica Set replset3 Up!");
//
//------------------------------------------------
// 3.6. replset4
//------------------------------------------------
//
// 3.6.1. Connect to mongod
//
db = connect("localhost:27300/my_database");
//
// 3.6.2. Initiate the replica set
//
res = rs.initiate(
    {
        "_id" : "replset4",
        "members" : [
            { _id:0,host:"localhost:"+"27300" },
            { _id:1,host:"localhost:"+"27301" },
            { _id:2,host:"localhost:"+"27302" }
        ]
    }
);
//
// 3.6.3. Wait until all nodes of the replica set are up and running
//
while (res.ok != 1){
    sleep(10);
}
print("Replica Set replset4 Created!");
while (((rs.status().members[0].state != 1) && (rs.status().members[0].state != 2)) || ((rs.status().members[1].state != 1) && (rs.status().members[1].state != 2)) || ((rs.status().members[2].state != 1) && (rs.status().members[2].state != 2))) {
    sleep(10);
}
print("Replica Set replset4 Up!");
//
//------------------------------------------------
// 3.7. Quit
//------------------------------------------------
//
quit()
