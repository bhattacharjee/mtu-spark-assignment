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
# FUNCTION replace_values_all_columns
# ------------------------------------------
def replace_values_all_columns(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Number2", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1), ('Bruce', 2, 2), ('Connor', 1, 3), ('Diarmuid', 4, 4) ], my_schema )

    # 3. Operation T1: We replace the values for all columns
    resultDF = inputDF.na.replace(1, -1)

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION replace_values_subset_columns
# ------------------------------------------
def replace_values_subset_columns(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Number2", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1), ('Bruce', 2, 2), ('Connor', 1, 3), ('Diarmuid', 4, 4) ], my_schema )

    # 3. Operation T1: We replace the values for all columns
    resultDF = inputDF.na.replace(1, -1, ["Number1"])

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
        print("\n\n--- Replace values in all columns ---\n\n")
        replace_values_all_columns(spark)

    if (option == 2):
        print("\n\n--- Replace values in a subset of columns ---\n\n")
        replace_values_subset_columns(spark)


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
    pass

    # 3. We configure the Spark Context
    pass

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark, option)
