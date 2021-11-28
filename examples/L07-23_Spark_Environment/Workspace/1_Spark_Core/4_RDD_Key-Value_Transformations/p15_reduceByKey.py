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
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection
    # [("One", 2), ("Three", 8), ("Two", 3), ("One", 7), ("Three", 5), ("Three", 6), ("Two", 10)] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([("One", 1),
                               ("One", 2),
                               ("One", 3),
                               ("One", 4),
                               ("One", 5),
                               ("One", 6),
                               ("One", 7),
                               ("One", 8),
                               ("One", 9),
                               ("Two", 1),
                               ("Two", 2),
                               ("Two", 3),
                               ("Four", 1)
                              ])

    # 2. Operation T1: Transformation 'reduceByKey', so as to get a new RDD ('reducedRDD') from inputRDD.

    # reduceByKey is a higher-order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C which must be of type (key, value).
    # It produces as a result a new collection C' of type (key, value).

    # The amount of items in C' is equal to the amount of different keys available in C.
    # For each item ('k', 'v') in C', the value 'v' is computed from the aggregation of ['V1', 'V2', ..., 'Vk'],
    # where ['V1', 'V2', ..., 'Vk'] are the values of all items ('k', 'Vi') in C with key equal to 'k'.

    # The function F is the one responsible of the aggregation.
    # It takes two values 'v1' and 'v2' and aggregates them producing a new value v'.
    # As we see, the function F must preserve the data type of 'v1' and 'v2', as the result v' has to be of the same data type.

    # To produce a single value 'v' in C' from the original values ['V1', 'V2', ..., 'Vk'] in C, the function F has to be applied k-1 times.
    # To improve the efficiency, each node containing a subset of ['V1', 'V2', ..., 'Vk'] runs the aggregation for its subset.
    # For example, if ['V1', 'V2', ..., 'Vk'] are distributed among 3 nodes {N1, N2, N3} each node reduces its local subset to a single value
    # {v'1, v'2, v'3}. Finally, the values {v'1, v'2, v'3} are transferred over the network to aggregate them into the final value v'.

    #         C1: parallelize             T1: reduceByKey
    # dataset -----------------> inputRDD -----------------> reducedRDD

    reducedRDD = inputRDD.reduceByKey(lambda x, y: x + y)

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: reduceByKey              A1: collect
    # dataset -----------------> inputRDD ----------------> reducedRDD ------------> resVAL

    resVAL = reducedRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


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
