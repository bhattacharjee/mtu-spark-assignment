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

# ------------------------------------------
# FUNCTION parse_node_line_info
# ------------------------------------------
def parse_node_line_info(line_info):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the node_id
    id = line_info[0]

    # 1.2. We output the number of items
    num_items = int(line_info[1])

    # 1.3. We output the number of partitions
    num_partitions = int(line_info[2])

    # 2. We remove the components consumed
    del line_info[2]
    del line_info[1]
    del line_info[0]

    # 3. We assign res
    res = (id, num_items, num_partitions, line_info)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION parse_partition_line_info
# ------------------------------------------
def parse_partition_line_info(line_info):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the node_id
    id = line_info[0]

    # 1.2. We output the number of items
    num_elements = int(line_info[1])

    # 1.3. We output the start of the key range
    start_key_range = line_info[2]

    # 1.4. We output the end of the key range
    end_key_range = line_info[3]

    # 2. We remove the components consumed
    del line_info[3]
    del line_info[2]
    del line_info[1]
    del line_info[0]

    # 3. We assign res
    res = (id, num_elements, start_key_range, end_key_range, line_info)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION read_metadata
# ------------------------------------------
def read_metadata(cluster_path):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the num_nodes
    num_nodes = 0

    # 1.2. We output the info of each node
    nodes_info = []

    # 1.3. We output the num_partitions
    num_partitions = 0

    # 1.4. We output the info of each partition
    partitions_info = []

    # 2. We open the file for reading
    my_input_stream = codecs.open(cluster_path + "Metadata/my_metadata.txt", "r", encoding="utf-8")

    # 3. We read the first line
    num_nodes = int(my_input_stream.readline().strip())

    # 4. We read the info from the nodes
    for index in range(num_nodes):
        # 4.1. We get the line info
        line_info = my_input_stream.readline().strip().split("\t")

        # 4.2. We process the line_info
        line_info_processed = parse_node_line_info(line_info)

        # 4.3. We append the line info processed
        nodes_info.append(line_info_processed)

    # 5. We read the next line
    num_partitions = int(my_input_stream.readline().strip())

    # 6. We read the info from the nodes
    for index in range(num_partitions):
        # 6.1. We get the line info
        line_info = my_input_stream.readline().strip().split("\t")

        # 6.2. We process the line_info
        line_info_processed = parse_partition_line_info(line_info)

        # 6.3. We append the line info processed
        partitions_info.append(line_info_processed)

    # 7. We close the file
    my_input_stream.close()

    # 8. We assign res
    res = (num_nodes, nodes_info, num_partitions, partitions_info)

    # 9. We return res
    return res

# ------------------------------------------
# FUNCTION write_metadata
# ------------------------------------------
def write_metadata(cluster_path, num_nodes, nodes_info, num_partitions, partitions_info):
    # 1. We open the metadata file for reading
    my_input_stream = codecs.open(cluster_path + "Metadata/my_metadata.txt", "w", encoding="utf-8")

    # 2. We write the line with the number of nodes
    my_input_stream.write(str(num_nodes) + "\n")

    # 3. We print a line per each node
    for info_tuple in nodes_info:
        my_str = info_tuple[0] + "\t" + str(info_tuple[1]) + "\t" + str(info_tuple[2]) + "\t" + "\t".join(info_tuple[3]) + "\n"
        my_input_stream.write(my_str)

    # 4. We write the line with the number of partitions
    my_input_stream.write(str(num_partitions) + "\n")

    # 5. We print a line per each partition
    for info_tuple in partitions_info:
        my_str = str(info_tuple[0]) + "\t" + str(info_tuple[1]) + "\t" + info_tuple[2] + "\t" + info_tuple[3]
        if (info_tuple[1] > 0):
            my_str = my_str + "\t" + "\t".join(info_tuple[4])
        my_str = my_str + "\n"
        my_input_stream.write(my_str)

    # 6. We close the metadata file
    my_input_stream.close()
