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
# FUNCTION inner_join_UDF
# ------------------------------------------
def inner_join_UDF(spark, my_dataset_dir1, my_dataset_dir2):
    # 1. We define the Schema of our DFs.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("age", pyspark.sql.types.IntegerType(), False)
        ])

    # 2. Operation C2: 'read' to create the DataFrame from the dataset and the schema
    input1_DF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir1)

    # 3. Operation C2: Creation CreateDataFrame
    input2_DF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir2)

    # 5. Operation T1: We get the inner join based in one column
    my_compareUDF = pyspark.sql.functions.udf(lambda x, y: x == y, pyspark.sql.types.BooleanType())
    resultDF = input1_DF.join(input2_DF,
                              my_compareUDF(input1_DF["age"], input2_DF["age"]),
                              "inner"
                             )

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation E1: Explain
    resultDF.explain()

    # 8. Operation A1: Collect
    resVAL = resultDF.collect()

    # 9. We print the result
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION inner_join_DSL
# ------------------------------------------
def inner_join_DSL(spark, my_dataset_dir1, my_dataset_dir2):
    # 1. We define the Schema of our DFs.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("age", pyspark.sql.types.IntegerType(), False)
        ])

    # 2. Operation C2: 'read' to create the DataFrame from the dataset and the schema
    input1_DF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir1)

    # 3. Operation C2: Creation CreateDataFrame
    input2_DF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir2)

    # 5. Operation T1: We get the inner join based in one column
    resultDF = input1_DF.join(input2_DF,
                              input1_DF["age"] == input2_DF["age"],
                              "inner"
                             )

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation E1: Explain
    resultDF.explain()

    # 8. Operation A1: Collect
    resVAL = resultDF.collect()

    # 9. We print the result
    for item in resVAL:
        print(item)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir1, my_dataset_dir2, option):
    if (option == 1):
        print("\n\n--- Inner join UDF ---\n\n")
        inner_join_UDF(spark, my_dataset_dir1, my_dataset_dir2)

    if (option == 2):
        print("\n\n--- Inner join DSL ---\n\n")
        inner_join_DSL(spark, my_dataset_dir1, my_dataset_dir2)

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

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    #my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    my_dataset_dir1 = "FileStore/tables/2_Spark_SQL/my_dataset1/"
    my_dataset_dir2 = "FileStore/tables/2_Spark_SQL/my_dataset2/"

    if local_False_databricks_True == False:
        my_dataset_dir1 = my_local_path + my_dataset_dir1
        my_dataset_dir2 = my_local_path + my_dataset_dir2
    else:
        my_dataset_dir1 = my_databricks_path + my_dataset_dir1
        my_dataset_dir2 = my_databricks_path + my_dataset_dir2

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir1, my_dataset_dir2, option)
