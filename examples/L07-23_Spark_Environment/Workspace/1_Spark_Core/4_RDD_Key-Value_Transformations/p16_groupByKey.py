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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, 2), (3, 8), (1, 4), (3, 4), (3, 6)] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, 2), (3, 8), (1, 4), (3, 4), (3, 6)])

    # 2. Operation T1: Transformation 'groupByKey', so as to get a new RDD ('groupedRDD') from inputRDD.

    # groupByKey is a higher-order function.
    # It requires as input argument: (i) a collection of items C which must be of type (key, value).
    # It produces as a result a new collection C' of type (key, [value]).

    # The amount of items in C' is equal to the amount of different keys available in C.
    # For each item ('k', 'v') in C', the value 'v' is computed from the list ['V1', 'V2', ..., 'Vk'],
    # where ['V1', 'V2', ..., 'Vk'] are the values of all items ('k', 'Vi') in C with key equal to 'k'.

    #         C1: parallelize             T1: groupByKey
    # dataset -----------------> inputRDD ----------------> groupedRDD

    groupedRDD = inputRDD.groupByKey()\
                         .mapValues( lambda x : sorted(list(x)) )

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: groupByKey              A1: collect
    # dataset -----------------> inputRDD ---------------> groupedRDD ------------> resVAL

    resVAL = groupedRDD.collect()

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
