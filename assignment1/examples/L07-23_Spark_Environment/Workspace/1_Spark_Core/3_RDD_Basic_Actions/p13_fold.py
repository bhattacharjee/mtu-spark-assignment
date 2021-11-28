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
# FUNCTION my_mult
# ------------------------------------------
def my_mult(x, y):
    # 1. We create the output variable
    res = x * y

    # 2. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
    # Please note that the name parallelize is a false friend here. Indeed, the entire RDD is to be stored in a single machine,
    # and must fit in memory.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4])

    # 2. Operation P1: We persist inputRDD, as we are going to use it more than once.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------

    inputRDD.persist()

    # 3. Operation A1: Action 'fold', so as to get one aggregated value from inputRDD.

    # The action operation 'fold' is a higher order function.
    # It is similar to reduce, but in addition takes a "zero value" to be used for the initial call on each partition. The zero value you provide
    # should be the identity element for your operation; that is, applying it multiple times with your function should not change
    # the value (e.g., 0 for +, 1 for *, or an empty list for concatenation).

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: fold
    #                                                    |------------> res1VAL
    #

    res1VAL = inputRDD.fold(0, lambda x, y: x + y)

    # 4. We print by the screen the result computed in res1VAL
    print(res1VAL)

    # 5. Operation A2: Action 'fold', so as to get one aggregated value from inputRDD.

    # In this case we define F with our own function.

    #         C1: parallelize             P1: persist    ------------
    # dataset -----------------> inputRDD -------------> | inputRDD |
    #                                                    ------------
    #                                                    |
    #                                                    | A1: fold
    #                                                    |------------> res1VAL
    #                                                    |
    #                                                    | A2: fold
    #                                                    |------------> res2VAL

    res2VAL = inputRDD.fold(1, my_mult)

    # 6. We print by the screen the result computed in res2VAL
    print(res2VAL)


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

