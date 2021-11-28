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
# FUNCTION all_columns_any_null_value
# ------------------------------------------
def all_columns_any_null_value(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, None), ('Bruce', None, True), ('Connor', None, None), (None, None, None) ], my_schema )

    # 3. Operation T1: We delete rows with any null value
    resultDF = inputDF.na.drop("any")

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION all_columns_all_null_values
# ------------------------------------------
def all_columns_all_null_values(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, None), ('Bruce', None, True), ('Connor', None, None), (None, None, None) ], my_schema )

    # 3. Operation T1: We delete rows with all null value
    resultDF = inputDF.na.drop("all")

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION subset_columns_all_null_values
# ------------------------------------------
def subset_columns_all_null_values(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, None), ('Bruce', None, True), ('Connor', None, None), (None, None, None) ], my_schema )

    # 3. Operation T1: We delete rows with all null values on the subset of columns being considered
    resultDF = inputDF.na.drop("all", None, ["Number", "LikesTennis"])

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option):
    if (option == 1):
        print("\n\n--- All columns, drop rows with any null value ---\n\n")
        all_columns_any_null_value(spark)

    if (option == 2):
        print("\n\n--- All columns, drop rows with all null values ---\n\n")
        all_columns_all_null_values(spark)

    if (option == 3):
        print("\n\n--- Subset of columns, drop rows with all null values ---\n\n")
        subset_columns_all_null_values(spark)


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
    option = 3

    # 2. Local or Databricks
    pass

    # 3. We configure the Spark Context
    pass

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark, option)
