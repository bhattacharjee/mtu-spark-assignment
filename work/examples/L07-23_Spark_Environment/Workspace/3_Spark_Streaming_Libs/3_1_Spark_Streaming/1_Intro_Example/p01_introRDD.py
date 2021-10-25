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
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, my_result_dir, longitude):
    # 1. Creation C1: textFile
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Transformation T1: filter
    solutionRDD = inputRDD.filter(lambda line: len(line) > longitude)

    # 3. Persist P1:
    solutionRDD.persist()

    # 4. Action A1: Collect
    resVAL = solutionRDD.collect()
    for item in resVAL:
        print(item)

    # 5. Action A2: We save to file
    solutionRDD.saveAsTextFile(my_result_dir)

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
    longitude = 15

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_1_Spark_Streaming/my_static_dataset/f.txt"
    my_result_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_1_Spark_Streaming/my_result/"

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

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, my_result_dir, longitude)
