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
import addition
import splitting
import migrating
import codecs
import os
import shutil
import bisect

# ------------------------------------------
# FUNCTION reset_cluster
# ------------------------------------------
def reset_cluster(cluster_path):
    # 1. We change the directory
    os.chdir(cluster_path)

    # 2. We get the list of files and directories
    dir_content = os.listdir()

    # 3. We remove all directories
    size = len(dir_content) - 1
    while (size >= 0):
        if (os.path.isdir(dir_content[size]) == True):
            shutil.rmtree(dir_content[size])
        size -= 1

    # 4. We recreate the Metadata and Node_1 directories
    os.mkdir("./Metadata/")
    os.mkdir("./Node_1/")

    # 5. We create the my_metadata.txt file
    my_input_stream = codecs.open("./Metadata/my_metadata.txt", "w", encoding="utf-8")
    my_input_stream.write("1\nNode_1\t0\t1\tPartition_1\n1\nPartition_1\t0\t" + chr(1) + "\t" + chr(1114111) + "\n")
    my_input_stream.close()

    # 6. We create the Partition_1.txt file
    my_input_stream = codecs.open("./Node_1/Partition_1.txt", "w", encoding="utf-8").close()

    # 7. We change the directory back
    os.chdir("../code/")

    # 8. We reset the log file
    my_input_stream = codecs.open(cluster_path + "my_log.txt", "w", encoding="utf-8").close()

# ------------------------------------------
# FUNCTION process_new_item
# ------------------------------------------
def process_new_item(dataset_path,
                     cluster_path,
                     partition_max_size,
                     node_max_size,
                     node_threshold,
                     new_item_index,
                     num_nodes,
                     nodes_info,
                     num_partitions,
                     partitions_info
                    ):

    # 1. We create the output variable
    res = ()

    # 1.1. We output the num_nodes
    num_nodes = num_nodes

    # 1.2. We output the num_partitions
    num_partitions = num_partitions

    # 2. We get the item to be added from the dataset
    item_info = addition.get_item_of_interest_from_dataset(dataset_path, new_item_index)

    # 3. We add the item to the cluster
    (split_needed, migrate_needed, node_id, node_index, partition_id, partition_index) = addition.process_addition(cluster_path,
                                                                                                                   num_nodes,
                                                                                                                   nodes_info,
                                                                                                                   num_partitions,
                                                                                                                   partitions_info,
                                                                                                                   item_info,
                                                                                                                   partition_max_size,
                                                                                                                   node_max_size,
                                                                                                                   node_threshold,
                                                                                                                   new_item_index
                                                                                                                  )

    # 4. If a partition split is needed, we go with it
    if (split_needed == True):
        splitting.process_splitting(cluster_path,
                                    nodes_info,
                                    num_partitions,
                                    partitions_info,
                                    node_id,
                                    node_index,
                                    partition_id,
                                    partition_index,
                                    new_item_index
                                   )
        num_partitions += 1

    # 5. If a partition migration is needed, we go with it
    if (migrate_needed == True):
        num_nodes = migrating.process_migrating(cluster_path,
                                                num_nodes,
                                                nodes_info,
                                                partitions_info,
                                                node_id,
                                                node_index,
                                                partition_max_size,
                                                node_max_size,
                                                node_threshold,
                                                new_item_index
                                               )

    # 6. We assign res
    res = (num_nodes, num_partitions)

    # 7. We return res
    return res
