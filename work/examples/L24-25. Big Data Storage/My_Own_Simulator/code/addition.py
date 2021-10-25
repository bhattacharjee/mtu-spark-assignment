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
import bisect

# ------------------------------------------
# FUNCTION get_item_of_interest_from_dataset
# ------------------------------------------
def get_item_of_interest_from_dataset(dataset_path, new_item_index):
    # 1. We create the output variable
    res = ()

    # 2. We open the dataset and discard all the lines prior to the one interesting us
    my_input_stream = codecs.open(dataset_path + "my_dataset.csv", "r", encoding="utf-8")
    for index in range(new_item_index):
        line = my_input_stream.readline()

    # 3. We read the line of interest
    my_line = my_input_stream.readline().strip().split(";")

    # 4. We turn the line into a tuple
    if (my_line != ['']):
        res = tuple(my_line)

    # 5. We return res
    return res

# -------------------------------------------------------
# FUNCTION find_node_and_partition_fitting_the_new_item
# -------------------------------------------------------
def find_node_and_partition_fitting_the_new_item(num_nodes, nodes_info, num_partitions, partitions_info, item_info):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the node_id
    node_id = ""

    # 1.2. We output the node_index
    node_index = 0

    # 1.3. We output the partition_id
    partition_id = ""

    # 1.4. We output the partition_index
    partition_index = 0

    # 2. We traverse the partitions until we find the one fitting the item based on its key ranges
    found = False
    while (partition_index < num_partitions) and (found == False):
        if (partitions_info[partition_index][2] < item_info[0]) and (item_info[0] < partitions_info[partition_index][3]):
            found = True
        else:
            partition_index += 1

    # 3. We also get the id of the partition hosting the new item
    partition_id = partitions_info[partition_index][0]

    # 4. We traverse the nodes until we find the one containing such partition
    found = False
    while (node_index < num_nodes) and (found == False):
        if (partition_id in nodes_info[node_index][3]):
            found = True
        else:
            node_index += 1

    # 5. We also get the id of the partition hosting the new item
    node_id = nodes_info[node_index][0]

    # 6. We assign res
    res = (node_id, node_index, partition_id, partition_index)

    # 7. We return res
    return res

# ------------------------------------------
# FUNCTION addition_update_partition_metadata
# ------------------------------------------
def addition_update_partition_metadata(new_key, partition_index, partitions_info, partition_max_size):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the index_for_writing
    index_for_writing = -1

    # 1.2. We ouptut whether a split is needed
    split_needed = False

    # 2. We access the partition info in list format
    my_info_list = list(partitions_info[partition_index])

    # 3. We remove the current tuple from partitions_info
    del partitions_info[partition_index]

    # 4. We get the index where to add the new key
    index_for_writing = bisect.bisect_left(my_info_list[4], new_key)

    # 5. We add the new key to the key_list
    my_info_list[4].insert(index_for_writing, new_key)

    # 6. We increase the number of items in the partition
    my_info_list[1] += 1

    # 7. If we reach the max amount of items in the partition we trigger the flag for splitting it
    if (my_info_list[1] == partition_max_size):
        split_needed = True

    # 8. We get the info back to tuple format
    my_info_tuple = tuple(my_info_list)

    # 9. We add the tuple back to partitions_info
    partitions_info.insert(partition_index, my_info_tuple)

    # 10. We assign res
    res = (index_for_writing, split_needed)

    # 11. We return res
    return res

# ------------------------------------------
# FUNCTION addition_update_node_metadata
# ------------------------------------------
def addition_update_node_metadata(node_index, nodes_info, node_max_size, node_threshold):
    # 1. We create the output variable
    res = False

    # 2. We access the node info in list format
    my_info_list = list(nodes_info[node_index])

    # 3. We remove the current tuple from nodes_info
    del nodes_info[node_index]

    # 4. We increase the number of items in the partition
    my_info_list[1] += 1

    # 5. If we reach the thresold items in the node we trigger the flag for migrating a partition
    if (my_info_list[1] >= int(node_max_size * node_threshold)):
        res = True

    # 6. We get the info back to tuple format
    my_info_tuple = tuple(my_info_list)

    # 7. We add the tuple back to node_info
    nodes_info.insert(node_index, my_info_tuple)

    # 8. We return res
    return res

# -----------------------------------------------------
# FUNCTION addition_update_cluster_file_with_new_item
# -----------------------------------------------------
def addition_update_cluster_file_with_new_item(cluster_path, node_id, partition_id, index_for_writing, item_info):
    # 1. We open the file for reading
    file_name = cluster_path + node_id + "/" + partition_id + ".txt"
    my_input_stream = codecs.open(file_name, "r", encoding="utf-8")

    # 2. We read it completely
    partition_text_file_content = my_input_stream.read().split("\n")

    # 3. We close the file
    my_input_stream.close()

    # 4. We re-open the file, now for writing
    my_input_stream = codecs.open(file_name, "w", encoding="utf-8")

    # 5. We re-write all the lines prior to the new one
    for index in range(index_for_writing):
        my_input_stream.write(partition_text_file_content[index] + "\n")

    # 6. We write the novel line
    my_input_stream.write(item_info[0] + ";" + item_info[1] + ";" + item_info[2] + "\n")

    # 7. We re-write all the lines after the new one
    for index in range(index_for_writing, len(partition_text_file_content) - 1):
        my_input_stream.write(partition_text_file_content[index] + "\n")

    # 8. We close the file
    my_input_stream.close()

# ------------------------------------------
# FUNCTION process_addition
# ------------------------------------------
def process_addition(cluster_path,
                     num_nodes,
                     nodes_info,
                     num_partitions,
                     partitions_info,
                     item_info,
                     partition_max_size,
                     node_max_size,
                     node_threshold,
                     new_item_index
                    ):

    # 1. We create the output variable
    res = ()

    # 1.1. We output whether there is a split needed
    split_needed = False

    # 1.2. We output whether there is a migration needed
    migrate_needed = False

    # 1.3. We output the node_id involved in the item addition
    node_id = ""

    # 1.4. We ouptut the node_index involved in the item addition
    node_index = 0

    # 1.5. We output the partition_id involved in the item addition
    partition_id = ""

    # 1.6. We ouptut the partition_index involved in the item addition
    partition_index = 0

    # 2. We look for the node and partition hosting the item
    (node_id, node_index, partition_id, partition_index) = find_node_and_partition_fitting_the_new_item(num_nodes,
                                                                                                        nodes_info,
                                                                                                        num_partitions,
                                                                                                        partitions_info,
                                                                                                        item_info
                                                                                                       )

    # 3. We update the partition metadata
    (index_for_writing, split_needed) = addition_update_partition_metadata(item_info[0], partition_index, partitions_info, partition_max_size)

    # 4. We update the node metadata
    migrate_needed = addition_update_node_metadata(node_index, nodes_info, node_max_size, node_threshold)

    # 5. We update the the cluster file
    addition_update_cluster_file_with_new_item(cluster_path, node_id, partition_id, index_for_writing, item_info)

    # 6. We add the info to the log file
    my_input_stream = codecs.open(cluster_path + "my_log.txt", "a+", encoding="utf-8")
    my_input_stream.write("ITEM " + str(new_item_index + 1) + ". ADDITION " + item_info[0] + " --> " + node_id + " --> " + partition_id + "\n")
    my_input_stream.close()

    # 7. We assign res
    res = (split_needed,
           migrate_needed,
           node_id,
           node_index,
           partition_id,
           partition_index
          )

    # 8. We return res
    return res