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
import pyspark.sql.functions


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir):
    # 1. Operation C1: We create the DataFrame from the dataset and the schema
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("value", pyspark.sql.types.StringType(), True)])

    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 2. Operation T1: We split the String by words
    word_listDF = inputDF.withColumn("wordList", pyspark.sql.functions.split(pyspark.sql.functions.col("value"), " ")) \
                         .drop("value")

    # 3. Operation T2: We explode the words separately
    wordsDF = word_listDF.withColumn("word", pyspark.sql.functions.explode(pyspark.sql.functions.col("wordList"))) \
                         .drop("wordList")

    # 4. Operation T3: We add the watermark on my_time
    aggDF = wordsDF.groupBy(pyspark.sql.functions.col("word")) \
                   .count()

    # 5. Operation T6: We sort them by the starting time of the window
    solutionDF = aggDF.orderBy(pyspark.sql.functions.col("count").desc())

    # 6. Operation A1: We collect the results
    resVAL = solutionDF.collect()
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
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/3_Spark_Streaming_Libs/3_2_Structured_Streaming/my_static_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir)
