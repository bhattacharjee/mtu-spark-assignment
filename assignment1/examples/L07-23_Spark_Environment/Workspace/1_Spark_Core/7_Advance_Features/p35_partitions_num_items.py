# Databricks notebook source
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
import pyspark

# ------------------------------------------
# FUNCTION size_with_mapPartitions
# ------------------------------------------
def size_with_mapPartitions(sc, my_dataset_dir):
    # 1. Operation C1: Creation textFile
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: We use mapPartitions to see how many partitions are there
    inputRDDSizes = inputRDD.mapPartitions( lambda partition_content_iterator : [sum(1 for item in partition_content_iterator)], True )

    # 3. Operation A1: Action collect
    resVAL = inputRDDSizes.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION size_with_mapPartitionsWithIndex
# ------------------------------------------
def size_with_mapPartitionsWithIndex(sc, my_dataset_dir):
    # 1. Operation C1: Creation textFile
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: We use mapPartitions to see how many partitions are there
    inputRDDSizes = inputRDD.mapPartitionsWithIndex( lambda partition_index, partition_content_iterator : [(partition_index, sum(1 for item in partition_content_iterator))], True )

    # 3. Operation A1: Action collect
    resVAL = inputRDDSizes.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_what_to_do_with_the_partition
# ------------------------------------------
def my_what_to_do_with_the_partition(partition_index, partition_content_iterator):
    # 1. We create the output variable
    res = [(partition_index, sum(1 for item in partition_content_iterator))]

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION size_with_inline_and_mapPartitionsWithIndex
# ------------------------------------------
def size_with_inline_and_mapPartitionsWithIndex(sc, my_dataset_dir):
    # 1. Operation C1: Creation textFile
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: We use mapPartitions to see how many partitions are there
    inputRDDSizes = inputRDD.mapPartitionsWithIndex( my_what_to_do_with_the_partition, True )

    # 3. Operation A1: Action collect
    resVAL = inputRDDSizes.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir):
    print("\n\n--- [BLOCK 1] mapPartitions to get the size of each partition ---")
    size_with_mapPartitions(sc, my_dataset_dir)

    print("\n\n--- [BLOCK 2] mapPartitionsWithIndex to get the index and size of each partition ---")
    size_with_mapPartitionsWithIndex(sc, my_dataset_dir)

    print("\n\n--- [BLOCK 3] mapPartitionsWithIndex with an Inline Function to get the index and size of each partition ---")
    size_with_inline_and_mapPartitionsWithIndex(sc, my_dataset_dir)
        
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
    pass

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/1_Spark_Core/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir)
