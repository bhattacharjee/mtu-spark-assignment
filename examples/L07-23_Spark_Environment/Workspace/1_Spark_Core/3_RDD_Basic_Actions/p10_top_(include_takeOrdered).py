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
# FUNCTION my_ordering
# ------------------------------------------
def my_ordering(x):
    # 1. We create the output variable
    res = x % 2

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4])

    # 2. Operation P1: We persist inputRDD, as we are going to use it more than once.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------

    inputRDD.persist()

    # 3. Operation A1: Action 'top', so as to take only a subset of the top items of the RDD.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: top
    #                                                    |---------> res1VAL     --- Iterator of items int ---

    res1VAL = inputRDD.top(2)

    # 4. We print by the screen the collection computed in res1VAL
    print("top(2):")
    for item in res1VAL:
        print(item)

    # 5. Operation A2: Action 'takeOrdered', so as to take only a subset of the top items of the RDD.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: top
    #                                                    |---------> res1VAL     --- Iterator of items int ---
    #                                                    |
    #                                                    | A2: takeOrdered
    #                                                    |-----------------> res2VAL     --- Iterator of items int ---

    res2VAL = inputRDD.takeOrdered(2, my_ordering)

    # 6. We print by the screen the collection computed in res2VAL
    print("takeOrdered(2)(my_ordering):")
    for item in res2VAL:
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

