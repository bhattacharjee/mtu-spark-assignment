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
def my_main(sc, my_dataset_dir, my_result_dir):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.

    # The dataset was previously loaded into DBFS by:
    # i) Place the desired dataset into your local file system folder %Test Environment%/my_dataset/
    # ii) Running the script "1.upload_dataset.bat"

    # Please note that, once again, the name textFile is a false friend here, as it seems that the parameter we are passing is the name of the file
    # we want to read. Indeed, the parameter here is the name of a folder, and the dataset consists on the content of the files of the folder.
    # An RDD of Strings is then generated, with one item per line of text in the files.
    # In this case, the RDD is distributed among different machines (nodes) of our cluster.

    #                            C1: textFile
    # dataset: DBFS inputFolder -------------> inputRDD

    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation A1: Action 'saveAsTextFile', so as to store the content of inputRDD into the desired DBFS folder.
    # If inputRDD is stored in 'm' nodes of our cluster, each node outputs a file 'part-XXXXX' to the DBFS outputFolder under a new file
    # parts-XXXXX (e.g., node 0 outputs the file part-00000, node 1 the file part-00001, and so on).
    # The whole content of the RDD being stored can be accesed by merging all output files.

    #                            C1: textFile           A1: saveAsTextFile
    # dataset: DBFS inputFolder -------------> inputRDD -------------------> DBFS outputFolder

    inputRDD.saveAsTextFile(my_result_dir)

    # 4. To access the result of DBFS outputFolder, we need to bring it back to our local machine by:
    #    i) Running the script "2.download_solution.bat", which places all part-XXXXX files into %Test Environment%/my_result/ and runs the
    #       Python program "3.merge_solutions.py", which merges all part-XXXXX files into the total solution file "solution.txt".


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
    my_result_dir = "FileStore/tables/1_Spark_Core/my_result/"

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
