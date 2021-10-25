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
import shutil
import os

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = []

    # 2. We set the line to be split by " "
    line = line.replace("\n", "")
    line = line.strip()
    line = line.rstrip()
    line = line.replace("\t", " ")

    # 3. We get rid of chars not being either a letter or a " "
    index = len(line) - 1

    # 3.1. We traverse all characters
    while (index >= 0):
        # 3.1.1. We get the ord of the character at position index
        char_val = ord(line[index])

        # 3.1.2. If (1) char_val is not " " and (2) char_val is not an Upper Case letter and (3) char_val is not a Lower Case letter
        if ( ( char_val != 32) and ((char_val < 65) or (char_val > 90)) and ((char_val < 97) or (char_val > 122)) ):
            # 3.1.2.1. We remove the index from the sentence
            line = line[:index] + line[(index+1):]
        # 3.1.3. If the character was an upper case letter
        elif ((char_val >= 65) and (char_val <= 90)):
            # 3.1.3.1. We add it as lower case
            line = line[:index] + chr(char_val + 32) + line[(index + 1):]

        # 3.1.4. We continue with the next index
        index = index - 1

    # 4. We get the list of words
    res = line.split(" ")

    index = len(res) - 1

    # 4.1. We traverse the words
    while (index >= 0):
        # 4.1.1. If it is empty, we remove it
        if (res[index] == ''):
            del res[index]

        # 4.1.2. We continue with the next word
        index = index - 1

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, my_result_dir):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset contained in the folder dataset_dir into an RDD.
    # If the dataset is big enough, its content is to be distributed among multiples nodes of the cluster.
    # The operation reads the files of the folder line by line. Thus, each item of the RDD is going to be a String (the content of the line being read).

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---

    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: Transformation 'flatMap', so as to get a new RDD ('allWordsRDD') with all the words of inputRDD.

    # We apply now a lambda expression as F to bypass each item of the collection to our actual filtering function F2 requiring more than
    # one argument. The function F2 is process_line, which cleans the lines from all bad_chars and splits it into a list of words.
    # We apply flatMap instead of map as we are not interested in the words of each line, just the words in general.
    # Thus, map would have given us an RDD where each item had been a list of words, the list of words on each line (i.e., each item had been [String]).
    # On the other hand, flatMap allows us to flat the lists and get instead an RDD where each item is a String, a word of the dataset.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> all_wordsRDD     --- RDD items are String ---

    allWordsRDD = inputRDD.flatMap(process_line)

    # 3. Operation T2: Transformation 'map', so as to get a new RDD (pairRDD) per word of the dataset.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---

    pairWordsRDD = allWordsRDD.map(lambda x: (x, 1))

    # 4. Operation T3: Transformation 'reduceByKey', so as to aggregate the amount of times each word appears in the dataset.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---
    #                                                    |
    #                                                    | T3: reduceByKey
    #                                                    |-----------------> countRDD    --- RDD items are (String, int) ---

    countRDD = pairWordsRDD.reduceByKey(lambda x, y: x + y)

    # 5. Operation T4: Transformation 'sortBy', so as to order the entries by decreasing order in the number of appearances.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---
    #                                                    |
    #                                                    | T3: reduceByKey
    #                                                    |-----------------> countRDD    --- RDD items are (String, int) ---
    #                                                                           |
    #                                                                           | T4: sortBy
    #                                                                           |-----------------> countRDD    --- RDD items are (String, int) ---

    solutionRDD = countRDD.sortBy(lambda x: x[1] * (-1) )

    # 6. Operation A1: Store the RDD solutionRDD into the desired folder from the DBFS.
    # Each node containing part of solutionRDD will produce a file part-XXXXX with such this RDD subcontent, where XXXXX is the name of the node.
    # Besides that, if the writing operation is successful, a file with name _SUCCESS will be created as well.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: flatMap
    #                        |------------> allWordsRDD     --- RDD items are String ---
    #                                       |
    #                                       | T2: map
    #                                       | ---------> pairWordsRDD     --- RDD items are (String, int) ---
    #                                                    |
    #                                                    | T3: reduceByKey
    #                                                    |-----------------> countRDD    --- RDD items are (String, int) ---
    #                                                                           |
    #                                                                           | T4: sortBy
    #                                                                           |-----------------> countRDD    --- RDD items are (String, int) ---
    #                                                                                                   |
    #                                                                                                   | S1: saveAsTextFile
    #                                                                                                   |--------------------> DBFS New Folder

    solutionRDD.saveAsTextFile(my_result_dir)

    # Extra: To debug the program execution, you might want to this three lines of code.
    # Each of them apply the action 'take', taking a few elements of each RDD being computed so as to display them by the screen.

    # resVAl = solutionRDD.take(10)
    # for item in resVAl:
    #  print(item)

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
    my_result_dir = "FileStore/tables/1_Spark_Core/my_result"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
        my_result_dir = my_local_path + my_result_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir
        my_result_dir = my_databricks_path + my_result_dir

    # 4. We remove my_result directory
    if local_False_databricks_True == False:
        if os.path.exists(my_result_dir):
            shutil.rmtree(my_result_dir)
    else:
        dbutils.fs.rm(my_result_dir, True)

    # 5. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 6. We call to our main function
    my_main(sc, my_dataset_dir, my_result_dir)
