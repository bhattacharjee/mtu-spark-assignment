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
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection ["This is my first line", "Hello", "Another line here"]
    # into an RDD. As we see, in this case our RDD is a collection of String items.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize(["This is my first line", "Hello", "Another line here"])

    # 2. Operation T1: Transformation 'sortBy', so as to get inputRDD sorted by the desired order we want.
    # The transformation operation 'sortBy' is a higher order function.
    # It requires as input arguments: (i) A function F and (ii) a collection of items C.
    # The function F specifies the weight we assign to each item.
    # In our case, given an RDD of String items, our function F will weight each item with the number of words that there are in the String.

    #         C1: parallelize             T1: sortBy
    # dataset -----------------> inputRDD ------------> sortedRDD

    sortedRDD = inputRDD.sortBy(lambda line: (-1) * len(line.split(" ")))

    # 3. Operation A1: collect the items from sortedRDD

    #         C1: parallelize             T1: sortBy              A1: collect
    # dataset -----------------> inputRDD ------------> sortedRDD -------------> resVAL

    resVAL = sortedRDD.collect()

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
