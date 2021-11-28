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

# ------------------------------------------
# FUNCTION createDataFrame_JSON_file_implicit_schema
# ------------------------------------------
def createDataFrame_JSON_file_implicit_schema(spark, my_dataset_file):
    # 1. Operation C1: Creation 'read'.
    inputDF = spark.read.load(my_dataset_file, format="json")

    # 2. Operation P1: Persistance of the DF
    inputDF.persist()

    # 3. Operation A1: Print the schema
    inputDF.printSchema()

    # 4. Operation A2: Action of displaying the content of the DF
    inputDF.show()

    # 4. Operation T1: Transformation withColumn
    newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 5. Operation A3: Action show
    newDF.show()

# ------------------------------------------
# FUNCTION createDataFrame_CSV_file_explicit_shema
# ------------------------------------------
def createDataFrame_CSV_file_explicit_shema(spark, my_dataset_file):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("city", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("age", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation 'read'
    inputDF = spark.read.format("csv") \
                        .option("delimiter", ";") \
                        .option("quote", "") \
                        .option("header", "false") \
                        .schema(my_schema) \
                        .load(my_dataset_file)

    # 3. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()

    # 4. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
    inputDF.show()

    # 5. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
    newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 6. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
    newDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option, my_dataset_file):
    if (option == 1):
        print("\n\n--- DataFrame from JSON File and Implicit Schema ---\n\n")
        createDataFrame_JSON_file_implicit_schema(spark, my_dataset_file)

    if (option == 2):
        print("\n\n--- DataFrame from CSV File and Explicit Schema ---\n\n")
        createDataFrame_CSV_file_explicit_shema(spark, my_dataset_file)

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
    option = 2

    file_name = ""
    if (option == 1):
        file_name = "my_example.json"
    else:
        file_name = "my_tiny_example.csv"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/2_Spark_SQL/my_dataset/" + file_name

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    pass

    # 5. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 6. We call to our main function
    my_main(spark, option, my_dataset_dir)
