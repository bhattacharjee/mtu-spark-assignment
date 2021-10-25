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
import bisect

# ------------------------------------------
# FUNCTION average_map_and_reduce
# ------------------------------------------
def average_map_and_reduce(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation T1: Transformation 'map', so as to get a new RDD ('pairRDD') from inputRDD.

    #         C1: parallelize             T1: map
    # dataset -----------------> inputRDD --------> pairRDD

    pairRDD = inputRDD.map(lambda x: (x, 1))

    # 3. Operation A1: Action 'reduce', so as to get one aggregated value from pairRDD.

    #         C1: parallelize             T1: map           A1: reduce
    # dataset -----------------> inputRDD --------> pairRDD ------------> resVAL

    resVAL = pairRDD.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # 4. We print by the screen the result computed in resVAL

    print("TotalSum = " + str(resVAL[0]))
    print("TotalItems = " + str(resVAL[1]))
    print("AverageValue = " + str((resVAL[0] * 1.0) / (resVAL[1] * 1.0)))


# ------------------------------------------
# FUNCTION aggregate_lambda
# ------------------------------------------
def aggregate_lambda(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

    # 2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.

    # The action operation 'aggregate' requires 3 input arguments:

    # (i). An initial zero value of the type we want to return.
    # It will be received as initial zero value on each node of the cluster. It serves as initial accumulator.

    # (ii). A function F1 to aggregate the RDD local elements of each node.
    # F1 must receive as input 2 parameters: The accumulator and the new local item to aggregate it with.

    # (iii). A function F2 to aggregate the different accumulators contained in the different nodes hosting the RDD.
    # F2 must receive as input 2 parameters: The final accumulator computed by Node1 and the final accumulator computed by Node2.
    #                                        This function states how to combine such these 2 accumulators.

    # In this case we define F1 and F2 via lambda abstractions.

    resVAL = inputRDD.aggregate((0, 0),
                                lambda acc, e: (acc[0] + e, acc[1] + 1),
                                lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
                               )

    #         C1: parallelize             A1: aggregate
    # dataset -----------------> inputRDD --------------> resVAL

    # 3. We print by the screen the collection computed in resVAL
    print("TotalSum = " + str(resVAL[0]))
    print("TotalItems = " + str(resVAL[1]))
    print("AverageValue = " + str((resVAL[0] * 1.0) / (resVAL[1] * 1.0)))

# ------------------------------------------
# FUNCTION my_string_aggregate
# ------------------------------------------
def my_string_aggregate(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD
    inputRDD = sc.parallelize([97, 98, 99, 100, 101, 102, 103, 104, 105])

    # 2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.

    # The action operation 'aggregate' requires 3 input arguments:

    # (i). An initial zero value of the type we want to return.
    # It will be received as initial zero value on each node of the cluster. It serves as initial accumulator.

    # (ii). A function F1 to aggregate the RDD local elements of each node.
    # F1 must receive as input 2 parameters: The accumulator and the new local item to aggregate it with.

    # (iii). A function F2 to aggregate the different accumulators contained in the different nodes hosting the RDD.
    # F2 must receive as input 2 parameters: The final accumulator computed by Node1 and the final accumulator computed by Node2.
    #                                        This function states how to combine such these 2 accumulators.

    # In this case we define F1 and F2 via lambda abstractions.

    resVAL = inputRDD.aggregate("",
                                lambda acc, e: acc + chr(e),
                                lambda acc1, acc2: acc1 + acc2
                               )

    #         C1: parallelize             A1: aggregate
    # dataset -----------------> inputRDD --------------> resVAL

    # 3. We print by the screen the collection computed in resVAL
    print(resVAL)

# ------------------------------------------
# FUNCTION combine_local_node
# ------------------------------------------
def combine_local_node(accum, item):
    # 1. We create the variable to return
    res = ()

    # 2. We modify the value of res
    val1 = accum[0] + item
    val2 = accum[1] + 1

    # 3. We assign res properly
    res = (val1, val2)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION combine_different_nodes
# ------------------------------------------
def combine_different_nodes(accum1, accum2):
    # 1. We create the variable to return
    res = ()

    # 2. We modify the value of res
    val1 = accum1[0] + accum2[0]
    val2 = accum1[1] + accum2[1]

    # 3. We assign res properly
    res = (val1, val2)

    # 3. We return res
    return res


# ------------------------------------------
# FUNCTION aggregate_own_functions
# ------------------------------------------
def aggregate_own_functions(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.

    #         C1: parallelize
    # dataset -----------------> inputRDD

    inputRDD = sc.parallelize([1, 2, 3, 4, 5])

    # 2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.

    # The action operation 'aggregate' requires 3 input arguments:

    # (i). An initial zero value of the type we want to return.
    # It will be received as initial zero value on each node of the cluster. It serves as initial accumulator.

    # (ii). A function F1 to aggregate the RDD local elements of each node.
    # F1 must receive as input 2 parameters: The accumulator and the new local item to aggregate it with.

    # (iii). A function F2 to aggregate the different accumulators contained in the different nodes hosting the RDD.
    # F2 must receive as input 2 parameters: The final accumulator computed by Node1 and the final accumulator computed by Node2.
    #                                        This function states how to combine such these 2 accumulators.

    # In this case we define F1 and F2 via our own functions.

    resVAL = inputRDD.aggregate((0, 0),
                                combine_local_node,
                                combine_different_nodes
                                )

    # 3. We print by the screen the collection computed in resVAL

    print("TotalSum = " + str(resVAL[0]))
    print("TotalItems = " + str(resVAL[1]))
    print("AverageValue = " + str((resVAL[0] * 1.0) / (resVAL[1] * 1.0)))

# ------------------------------------------
# FUNCTION get_accum_per_node
# ------------------------------------------
def get_accum_per_node(accum, word):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the dictionary
    my_dict = accum[0]

    # 1.2. We output the number of words
    num_words = accum[1]

    # 2. We get the first letter from word
    letter = word[0]

    # 3. If the letter is not in the dictionary
    if (letter not in my_dict):
        # 3.1. We add the letter to the dictionary, in this case with just this word
        my_dict[letter] = [ word ]

        # 3.2. As it is for sure a new letter, we increase the number of words
        num_words = num_words + 1

    # 4. If the letter is in the dictionary
    else:
        # 4.1. We find the index to add at
        index = bisect.bisect_left(my_dict[letter], word)

        # 4.2. If the word was not there
        if (index == len(my_dict[letter])) or (my_dict[letter][index] != word):
            # 4.2.1. We add the word to the list at that index
            my_dict[letter].insert(index, word)

            # 4.2.2. As it is for sure a new letter, we increase the number of words
            num_words = num_words + 1

    # 5. We assign res
    res = (my_dict, num_words)

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION combine_accum_of_nodes
# ------------------------------------------
def combine_accum_of_nodes(accum1, accum2):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the dictionary
    my_dict = accum1[0]

    # 1.2. We output the number of words
    num_words = accum1[1] + accum2[1]

    # 2. We kept the dictionary of accum2
    other_dict = accum2[0]

    # 3. We traverse the content of such dictionary
    for letter in other_dict:
        # 3.1. If letter was not in the first dictionary, we create the entry
        if (letter not in my_dict):
            my_dict[letter] = other_dict[letter]

        # 3.2. If the letter was on the first dictionary
        else:
            # 3.2.1. We traverse the list of words
            for word in other_dict[letter]:
                # 3.2.1.1. We find the index to add at
                index = bisect.bisect_left(my_dict[letter], word)

                # 3.2.1.2. If the word was not there
                if (index == len(my_dict[letter])) or (my_dict[letter][index] != word):
                    # 3.2.1.2.1 We add the word to the list at that index
                    my_dict[letter].insert(index, word)

                # 3.2.1.3. If the word was already there
                else:
                    # 3.2.1.3.1. As we have counted the word, we decrease it now
                    num_words = num_words - 1

    # 4. We assign res
    res = (my_dict, num_words)

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION different_datatypes_aggregate
# ------------------------------------------
def different_datatypes_aggregate(sc):
    # 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.
    inputRDD = sc.parallelize(["Hello", "Goodbye", "Hi", "Danke", "Hola", "Hello", "Hello", "Grand"])

    # 2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.
    resVAL = inputRDD.aggregate(({}, 0),
                                get_accum_per_node,
                                combine_accum_of_nodes
                                )

    # 3. We print by the screen the collection computed in resVAL
    my_dict = resVAL[0]
    num_words = resVAL[1]

    print("--- WORDS ---")
    for letter in my_dict:
        print(letter, end="")
        print(": ", end="")
        print(my_dict[letter])
    print("--- NUM WORDS ---")
    print(num_words)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    print("\n\n--- [BLOCK 1] Compute Average via map + reduce ---")
    average_map_and_reduce(sc)

    print("\n\n--- [BLOCK 2] Compute Average via aggregate with lambda ---")
    aggregate_lambda(sc)

    print("\n\n--- [BLOCK 3] Compute Average via aggregate with our own functions ---")
    aggregate_own_functions(sc)

    print("\n\n--- [BLOCK 4] Compute Different Datatypes Average ---")
    different_datatypes_aggregate(sc)

    print("\n\n--- [BLOCK 5] Compute Different Datatypes Average ---")
    my_string_aggregate(sc)

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

