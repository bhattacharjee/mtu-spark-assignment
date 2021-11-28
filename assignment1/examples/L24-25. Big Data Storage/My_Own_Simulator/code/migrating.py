#!/usr/local/bin/python3.8
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import codecs
import os
import shutil
import bisect

# ------------------------------------------
# FUNCTION get_partition_to_be_migrated
# ------------------------------------------
def get_partition_to_be_migrated(node_index, nodes_info, partitions_info, partition_max_size):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the partition_id
    partition_id = ""

    # 1.2. We output the partition_index
    partition_index = 0

    # 1.3. We output the partition_size
    partition_size = partition_max_size + 1

    # 2. We get the list of partitions
    partition_candidates = nodes_info[node_index][3]

    # 3. We traverse the list of partitions, looking for the one with min items
    for info_tuple in partitions_info:
        # 3.1. If the partition is one of our candidates and its number of items is smaller than the best one found so far
        if (info_tuple[0] in partition_candidates) and (info_tuple[1] < partition_size):
            # 3.1.1. We identify the partition id
            partition_id = info_tuple[0]

            # 3.1.2. We get its index in the partition list for the node
            partition_index = partition_candidates.index(partition_id)

            # 3.1.3. We update our best number found so far
            partition_size = info_tuple[1]

    # 4. We assign res
    res = (partition_id, partition_index, partition_size)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION get_node_to_be_migrated_to
# ------------------------------------------
def get_node_to_be_migrated_to(node_id,
                               num_nodes,
                               nodes_info,
                               partition_size,
                               node_max_size,
                               node_threshold
                              ):

    # 1. We create the ouptut variable
    res = ()

    # 1.1. We output the new_node_id
    new_node_id = ""

    # 1.2. We output the new_node_index
    new_node_index = 0

    # 2. We traverse the list of partitions, looking for the one with min items
    best_size = node_max_size + 1
    index = 0
    for info_tuple in nodes_info:
        # 2.1. If the partition is one of our candidates and its number of items is smaller than the best one found so far
        if (info_tuple[0] != node_id) and (info_tuple[1] < best_size):
            # 2.1.1. We identify the node id
            new_node_id = info_tuple[0]

            # 2.1.2. We get its index in the nodes list
            new_node_index = index

            # 2.1.3. We update our best number found so far
            best_size = info_tuple[1]

        # 2.2. We increase the index
        index += 1

    # 3. We check if the new node can fit the partition
    if ((best_size + partition_size) >= int(node_max_size * node_threshold)):
        # 3.1. We auto-scale and set the name of the new node
        new_node_id = "Node_" + str(num_nodes + 1)

        # 3.2. We auto-scale and set the index this new node will have in the list
        new_node_index = num_nodes

    # 4. We assign res
    res = (new_node_id, new_node_index)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION migrating_update_node_metadata
# ------------------------------------------
def migrating_update_node_metadata(num_nodes,
                                   nodes_info,
                                   node_index,
                                   partition_id,
                                   partition_index,
                                   partition_size,
                                   new_node_id,
                                   new_node_index
                                  ):

    # 1. We create the output variable
    res = num_nodes

    # 2. We access the node info in list format and delete it from the list
    my_info_list_1 = list(nodes_info[node_index])
    del nodes_info[node_index]

    # 3. We modify its fields
    my_info_list_1[1] = my_info_list_1[1] - partition_size
    my_info_list_1[2] = my_info_list_1[2] - 1
    del my_info_list_1[3][partition_index]

    # 4. We get the info back to tuple format
    my_info_tuple_1 = tuple(my_info_list_1)

    # 5. We add the tuple back to partitions_info
    nodes_info.insert(node_index, my_info_tuple_1)

    # 6. If the partition is migrated to a new node
    if (new_node_index == num_nodes):
        # 4.1. We increase the number of nodes
        res += 1

        # 4.2. We initialise the new node info
        my_info_list_2 = [new_node_id, partition_size, 1, [partition_id]]

    # 7. Otherwise, we access and modify the existing one
    else:
        my_info_list_2 = list(nodes_info[new_node_index])
        del nodes_info[new_node_index]
        my_info_list_2[1] = my_info_list_2[1] + partition_size
        my_info_list_2[2] = my_info_list_2[2] + 1
        my_order_index = bisect.bisect_left(my_info_list_2[3], partition_id)
        my_info_list_2[3].insert(my_order_index, partition_id)

    # 8. We get the info back to tuple format
    my_info_tuple_2 = tuple(my_info_list_2)

    # 9. We add the tuple back to partitions_info
    nodes_info.append(my_info_tuple_2)

    # 10. We return res
    return res

# ------------------------------------------
# FUNCTION migrating_update_cluster_files
# ------------------------------------------
def migrating_update_cluster_files(cluster_path, node_id, partition_id, new_node_id):
    # 1. We change the directory
    os.chdir(cluster_path)

    # 2. We get the list of files and directories
    dir_content = os.listdir()

    # 3. If the new_node_id is not a directory, we create it
    if (new_node_id not in dir_content):
        os.mkdir("./" + new_node_id)

    # 4. We change the directory back
    os.chdir("../code/")

    # 5. We copy the file
    shutil.move(cluster_path + node_id + "/" + partition_id + ".txt", cluster_path + new_node_id + "/" + partition_id + ".txt")

# ------------------------------------------
# FUNCTION process_migrating
# ------------------------------------------
def process_migrating(cluster_path,
                      num_nodes,
                      nodes_info,
                      partitions_info,
                      node_id,
                      node_index,
                      partition_max_size,
                      node_max_size,
                      node_threshold,
                      new_item_index
                     ):

    # 1. We create the ouptut variable
    res = num_nodes

    # 1. We identify the partition to be migrated
    (partition_id, partition_index, partition_size) = get_partition_to_be_migrated(node_index, nodes_info, partitions_info, partition_max_size)

    # 2. We identify the node to be migrated to
    (new_node_id, new_node_index) = get_node_to_be_migrated_to(node_id,
                                                               num_nodes,
                                                               nodes_info,
                                                               partition_size,
                                                               node_max_size,
                                                               node_threshold
                                                              )

    # 3. We update the node metadata
    res = migrating_update_node_metadata(num_nodes,
                                         nodes_info,
                                         node_index,
                                         partition_id,
                                         partition_index,
                                         partition_size,
                                         new_node_id,
                                         new_node_index,
                                        )

    # 4. We update the the cluster file
    migrating_update_cluster_files(cluster_path, node_id, partition_id, new_node_id)

    # 5. We register the new info in the log file

    # 5.1. We open the file
    my_input_stream = codecs.open(cluster_path + "my_log.txt", "a+", encoding="utf-8")

    # 5.2. If there is an auto-scale, we register it
    if (res > num_nodes):
        my_input_stream.write("ITEM " + str(new_item_index + 1) + ". AUTO-SCALE. NODE " + str(res) + " CREATED\n")

    # 5.3. We register the migration
    my_input_stream.write("ITEM " + str(new_item_index + 1) + ". MIGRATION FROM (" + node_id + ", " + partition_id + ") TO (" + new_node_id + ", " + partition_id + ")\n")

    # 5.4. We close the file
    my_input_stream.close()

    # 6. We return res
    return res
