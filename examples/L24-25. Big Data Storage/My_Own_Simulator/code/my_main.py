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
import metadata_io
import cluster_simulation
import sys

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_path,
            cluster_path,
            partition_max_size,
            node_max_size,
            node_threshold,
            num_items_to_process
           ):

    # 1. We reset the cluster
    cluster_simulation.reset_cluster(cluster_path)

    # 2. We read the metadata file
    (num_nodes, nodes_info, num_partitions, partitions_info) = metadata_io.read_metadata(cluster_path)

    # 3. We process the n required items
    for my_item_index in range(num_items_to_process):
        (num_nodes, num_partitions) = cluster_simulation.process_new_item(dataset_path,
                                                                          cluster_path,
                                                                          partition_max_size,
                                                                          node_max_size,
                                                                          node_threshold,
                                                                          my_item_index,
                                                                          num_nodes,
                                                                          nodes_info,
                                                                          num_partitions,
                                                                          partitions_info
                                                                          )

    # 4. We write the metadata file
    metadata_io.write_metadata(cluster_path, num_nodes, nodes_info, num_partitions, partitions_info)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed

    # 1.1. Path to the dataset
    dataset_path = "../Dataset_Generation/"

    # 1.2. Path to the cluster
    cluster_path = "../Cluster_Simulation/"

    # 1.3. The partition/chunk max size
    partition_max_size = 50

    # 1.4. The node/shard max size
    node_max_size = 200

    # 1.5. The node/shard threshold
    node_threshold = 0.75

    # 1.6. Num Items to Process --- Max value is 998 (size of the dataset)
    num_items_to_process = 150

    # 1.8. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        dataset_path = sys.argv[1]
        cluster_path = sys.argv[2]
        partition_max_size = int(sys.argv[3])
        node_max_size = int(sys.argv[4])
        node_threshold = float(sys.argv[5])
        num_items_to_process = int(sys.argv[6])

    # 2. We call to my_main
    my_main(dataset_path,
            cluster_path,
            partition_max_size,
            node_max_size,
            node_threshold,
            num_items_to_process
           )
