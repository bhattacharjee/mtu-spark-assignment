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
# FUNCTION my_map_values
# ------------------------------------------
def my_map_values(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "hello world"), (2, "bye bye"), (1, "nice to see you")]
    # into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "hello world"), (2, "bye bye"), (1, "hello nice to see you")])

    # 2. Operation T1: Transformation 'mapValues', so as to get a the lenght of each value per (key, value) in inputRDD.

    # mapValues is a higher-order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C which must be of type (key, value).
    # If applies F(value) to each item of the collection, thus producing as a result a new collection C' of type (key, value).

    # Please note that the function do not alter the keys of the collection items.
    # Thus, in terms of efficiency, the operation respects the existing partitions in the RDD.

    #         C1: parallelize             T1: reduceByKey
    # dataset -----------------> inputRDD -----------------> reducedRDD

    mappedRDD = inputRDD.mapValues(lambda x: len(x.split(" ")))

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: mapValues                A1: collect
    # dataset -----------------> inputRDD ----------------> mappedRDD ------------> resVAL

    resVAL = mappedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_flat_map_values
# ------------------------------------------
def my_flat_map_values(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "hello world"), (2, "bye bye"), (1, "nice to see you")]
    # into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "hello world"), (2, "bye bye"), (1, "hello nice to see you")])

    # 2. Operation T1: Transformation 'flatMapValues', so as to get a the lenght of each value per (key, value) in inputRDD.

    # flatMapValues is a higher-order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C which must be of type (key, value).
    # If applies F(value) to each item of the collection.
    # The application of F over value is supposed to return an iterator I[value1, value2, ..., valuek].
    # Thus, flatMapValues produces a new entry (key, valuei) per value in I.

    # Please note that the function do not alter the keys of the collection items.
    # Thus, in terms of efficiency, the operation respects the existing partitions in the RDD.

    #         C1: parallelize             T1: reduceByKey
    # dataset -----------------> inputRDD -----------------> reducedRDD

    mappedRDD = inputRDD.flatMapValues(lambda x: x.split(" "))

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: mapValues                A1: collect
    # dataset -----------------> inputRDD ----------------> mappedRDD ------------> resVAL

    resVAL = mappedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

    # ------------------------------------------


# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    print("\n\n--- [BLOCK 1] mapValues ---")
    my_map_values(sc)

    print("\n\n--- [BLOCK 2] flatMapValues ---")
    my_flat_map_values(sc)


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
    pass

    # 3. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We call to my_main
    my_main(sc)
