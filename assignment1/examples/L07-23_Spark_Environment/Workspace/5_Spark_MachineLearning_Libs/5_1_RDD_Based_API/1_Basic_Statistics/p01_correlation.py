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

import pyspark

import pyspark.mllib.stat

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We turn it into a tuple of integers
    size = len(params)
    for index in range(size):
        if (params[index] != ""):
            params[index] = int(params[index])
        else:
            params[index] = 0
    res = tuple(params)

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, correlation_method):
    # 1. Operation C1: We create the RDD from the dataset
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: We get an RDD of tuples
    infoRDD = inputRDD.map(process_line)

    # 3. Operation A1: We get the correlation matrix
    correlation_matrix = pyspark.mllib.stat.Statistics.corr(infoRDD, method=correlation_method)

    # 4. We display the result
    print(correlation_matrix)

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
    correlation_method = "pearson"
    #correlation_method = "spearman"

    #dataset_file_name = "pearson_2_vars_dataset.csv"
    dataset_file_name = "pearson_3_vars_dataset.csv"
    #dataset_file_name = "spearman_2_vars_dataset.csv"

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/5_Spark_MachineLearning_Libs/1_Basic_Statistics/" + dataset_file_name

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, correlation_method)
