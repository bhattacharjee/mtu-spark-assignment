# Databricks notebook source
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
def my_main(sc, my_dataset_dir):
    # 1. Operation C1: Creation 'textFile'
    inputRDD  = sc.textFile(my_dataset_dir)

    # 2. Operation T1: Transformation 'flatMap'
    allWordsRDD = inputRDD.flatMap(process_line)    
    
    # 2. Operation T1: Transformation 'map'
    pairWordsRDD = allWordsRDD.map( lambda x : (x, 1) )
    
    # 3. Operation T3: Transformation 'reduceByKey'
    countRDD = pairWordsRDD.reduceByKey( lambda x, y : x + y )

    # 4. Operation T4: Transformation 'map', to screw up the partitioner
    swapTupleRDD = countRDD.map( lambda x : (x[1], x[0]) ) 
    
    # 5. Operation T5: Transformation 'filter', to reduce the amount of words
    filteredRDD = swapTupleRDD.filter( lambda x : x[0] > 10000 ) 
    
    # 6. Operation T6: Transformation 'sortByKey', so as to order the entries by decreasing order in the number of appearances.
    solutionRDD = filteredRDD.sortByKey()
    
    # 7. Operation P1: Persist
    solutionRDD.persist()
    
    # 8. Operation A1: Action 'collect'
    resVAL1 = solutionRDD.collect()
    for item in resVAL1:
      print(item) 
    
    # 9. Operation A2: Action 'count'
    resVAL2 = solutionRDD.count()
    print(resVAL2) 
        
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
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/1_Spark_Core/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir)
