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

# -----------------------------------------------
# FUNCTION splitting_update_partition_metadata
# -----------------------------------------------
def splitting_update_partition_metadata(num_partitions, partition_index, partitions_info):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the name of the new partition
    new_partition_id = ""

    # 1.2. We output the index for splitting
    index_for_splitting = 0

    # 2. We access the partition info in list format and delete it from the list
    my_info_list_1 = list(partitions_info[partition_index])
    del partitions_info[partition_index]

    # 3. We get a fresh copy of my_info_list_1
    my_info_list_2 = [0] * 5
    for i in range(4):
        my_info_list_2[i] = my_info_list_1[i]
    my_info_list_2[4] = []
    for i in range(len(my_info_list_1[4])):
        my_info_list_2[4].append(my_info_list_1[4][i])

    # 4. We update the first field
    new_partition_id = "Partition_" + str(num_partitions + 1)
    my_info_list_2[0] = new_partition_id

    # 5. We update the second field
    index_for_splitting = my_info_list_1[1] // 2
    my_info_list_1[1] = index_for_splitting
    my_info_list_2[1] = my_info_list_2[1] - index_for_splitting

    # 6. We update the third and fourth fields
    my_info_list_1[3] = my_info_list_1[4][index_for_splitting]
    my_info_list_2[2] = my_info_list_1[4][index_for_splitting]

    # 7. We update the fifth field
    my_info_list_1[4] = my_info_list_1[4][0:index_for_splitting]
    my_info_list_2[4] = my_info_list_2[4][index_for_splitting:]

    # 8. We get the info back to tuple format
    my_info_tuple_1 = tuple(my_info_list_1)
    my_info_tuple_2 = tuple(my_info_list_2)

    # 9. We add the tuple back to partitions_info
    partitions_info.insert(partition_index, my_info_tuple_1)
    partitions_info.append(my_info_tuple_2)

    # 10. We assign res
    res = (new_partition_id, index_for_splitting)

    # 11. We return res
    return res

# -----------------------------------------------
# FUNCTION splitting_update_node_metadata
# -----------------------------------------------
def splitting_update_node_metadata(node_index, nodes_info, new_partition_id):
    # 1. We access the partition info in list format and delete it from the list
    my_info_list = list(nodes_info[node_index])
    del nodes_info[node_index]

    # 3. We update the third field
    my_info_list[2] = my_info_list[2] + 1

    # 4. We update the fourth field
    my_info_list[3].append(new_partition_id)

    # 5. We get the info back to tuple format
    my_info_tuple = tuple(my_info_list)

    # 6. We add the tuple back to nodes_info
    nodes_info.insert(node_index, my_info_tuple)

# -----------------------------------------------------
# FUNCTION splitting_update_cluster_files
# -----------------------------------------------------
def splitting_update_cluster_files(cluster_path, node_id, partition_id, new_partition_id, index_for_splitting):
    # 1. We open the file for reading
    file_name = cluster_path + node_id + "/" + partition_id + ".txt"
    my_input_stream = codecs.open(file_name, "r", encoding="utf-8")

    # 2. We read it completely
    partition_text_file_content = my_input_stream.read().split("\n")

    # 3. We close the file
    my_input_stream.close()

    # 4. We re-open the file, now for writing
    my_input_stream = codecs.open(file_name, "w", encoding="utf-8")

    # 5. We write all the elements belonging to it
    for index in range(index_for_splitting):
        my_input_stream.write(partition_text_file_content[index] + "\n")

    # 6. We close the file
    my_input_stream.close()

    # 7. We open the new file
    new_file_name = cluster_path + node_id + "/" + new_partition_id + ".txt"
    my_input_stream = codecs.open(new_file_name, "w", encoding="utf-8")

    # 7. We write all the elements belonging to it
    for index in range(index_for_splitting, len(partition_text_file_content) - 1):
        my_input_stream.write(partition_text_file_content[index] + "\n")

    # 8. We close the file
    my_input_stream.close()

# ------------------------------------------
# FUNCTION process_splitting
# ------------------------------------------
def process_splitting(cluster_path,
                      nodes_info,
                      num_partitions,
                      partitions_info,
                      node_id,
                      node_index,
                      partition_id,
                      partition_index,
                      new_item_index
                     ):

    # 1. We update the partition metadata
    (new_partition_id, index_for_splitting) = splitting_update_partition_metadata(num_partitions, partition_index, partitions_info)

    # 2. We update the node metadata
    splitting_update_node_metadata(node_index, nodes_info, new_partition_id)

    # 3. We update the the cluster file
    splitting_update_cluster_files(cluster_path, node_id, partition_id, new_partition_id, index_for_splitting)

    # 4. We add the info to the log file
    my_input_stream = codecs.open(cluster_path + "my_log.txt", "a+", encoding="utf-8")
    my_input_stream.write("ITEM " + str(new_item_index + 1) + ". SPLITTING " + partition_id + " INTO " + partition_id + " AND " + new_partition_id + "\n")
    my_input_stream.close()
