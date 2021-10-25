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
# FUNCTION my_keys
# ------------------------------------------
def my_keys(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "A"), (1, "B"), (2, "B")] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "A"), (1, "B"), (2, "B")])

    # 2. Operation T1: Transformation 'keys', so as to get a new RDD with the keys in inputRDD.

    #         C1: parallelize             T1: keys
    # dataset -----------------> inputRDD ----------> keysRDD

    keysRDD = inputRDD.keys()

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: keys            A1: collect
    # dataset -----------------> inputRDD ---------> keysRDD ------------> resVAL

    resVAL = keysRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)


# ------------------------------------------
# FUNCTION my_values
# ------------------------------------------
def my_values(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "A"), (1, "B"), (2, "B")] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([(1, "A"), (1, "B"), (2, "B")])

    # 2. Operation T1: Transformation 'values', so as to get a new RDD with the values in inputRDD.

    #         C1: parallelize             T1: values
    # dataset -----------------> inputRDD -----------> valuesRDD

    valuesRDD = inputRDD.values()

    # 3. Operation A1: 'collect'.

    #         C1: parallelize             T1: values              A1: collect
    # dataset -----------------> inputRDD -----------> valuesRDD ------------> resVAL

    resVAL = valuesRDD.collect()

    # 4. We print by the screen the collection computed in resVAL
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    print("\n\n--- [BLOCK 1] keys ---")
    my_keys(sc)

    print("\n\n--- [BLOCK 2] values ---")
    my_values(sc)


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
