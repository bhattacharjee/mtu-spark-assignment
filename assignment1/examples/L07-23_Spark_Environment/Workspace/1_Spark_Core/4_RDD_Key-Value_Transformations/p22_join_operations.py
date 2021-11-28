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
def my_main(sc, option):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [("A", 1), ("A", 2), ("B", 1), ("C", 1)] into an RDD.

    #          C1: parallelize
    # dataset1 -----------------> input1RDD

    input1RDD = sc.parallelize([("A", 1), ("A", 2), ("B", 1), ("C", 1)])

    # 2. Operation C2: Creation 'parallelize', so as to store the content of the collection [("A", 1.0), ("B", 1.0), ("B", 2.0), ("D", 1.0)] into an RDD.

    #          C1: parallelize
    # dataset1 -----------------> input1RDD
    #          C2: parallelize
    # dataset2 -----------------> input2RDD

    input2RDD = sc.parallelize([("A", True), ("B", True), ("B", False), ("D", True)])

    # 3. Operation T1: We apply a set transformation based on the option being passed

    #          C1: parallelize
    # dataset1 -----------------> input1RDD ---| T1: set operation
    #          C2: parallelize                 |---------------------> setResultRDD
    # dataset2 -----------------> input2RDD ---|

    # OPTION 1: join
    # Only keys that are present in both pair RDDs are output.
    # When there are multiple values for the same key in one of the inputs,
    # the resulting pair RDD will have an entry for every possible pair of values with that key from the two input RDDs.
    if (option == 1):
        setResultRDD = input1RDD.join(input2RDD)

    # OPTIONS 2 AND 3: leftOuterJoin and rightOuterJoin
    # Sometimes we don't need the key to be present in both RDDs to want it in our result.
    # leftOuterJoin(other) and rightOuterJoin(other) both join pair RDDs together by key,
    # where one of the pair RDDs can be missing the key.

    # With leftOuterJoin the resulting pair RDD has entries for each key in input1RDD.
    # If a key is not in input2RDD, then the value None is used.
    if (option == 2):
        setResultRDD = input1RDD.leftOuterJoin(input2RDD)

    # With rightOuterJoin the resulting pair RDD has entries for each key in input2RDD.
    # If a key is not in input1RDD, then the value None is used.
    if (option == 3):
        setResultRDD = input1RDD.rightOuterJoin(input2RDD)

    # OPTION 4: cogroup
    # It groups data from from input1RDD and input2RDD sharing the same key.
    # It can be used for much more than just implementing joins. We can also use it to implement intersect by key.
    if (option == 4):
        setResultRDD = input1RDD.cogroup(input2RDD)

    # 4. Operation A1: collect the result

    #          C1: parallelize
    # dataset1 -----------------> input1RDD ---| T1: set operation                  A1: collect
    #          C2: parallelize                 |---------------------> setResultRDD ------------> resVAL
    # dataset2 -----------------> input2RDD ---|

    resVAL = setResultRDD.collect()

    # 5. We print by the screen the collection computed in resVAL
    if option != 4:
        for item in resVAL:
            print("(" + str(item[0]) + ", " + str(item[1]) + ")")
    else:
        for item in resVAL:
            my_line = "(" + str(item[0]) + ", ["

            if (len(item[1][0]) > 0):
                for val in item[1][0]:
                    my_line = my_line + str(val) + ","
                my_line = my_line[:-1]

            my_line = my_line + "], ["

            if (len(item[1][1]) > 0):
                for val in item[1][1]:
                    my_line = my_line + str(val) + ","
                my_line = my_line[:-1]

            my_line = my_line + "])"

            print(my_line)

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
    option = 4

    # 2. Local or Databricks
    pass

    # 3. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We call to my_main
    my_main(sc, option)
