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
# FUNCTION get_previous_row_value
# ------------------------------------------
def get_previous_row_value(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Val1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val2", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val3", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1, 10), ('Barry', 1, 1, 15), ('Connor', 1, 1, 20), ('Diarmuid', 1, 1, 25),
                                       ('Emma', 2, 4, 30), ('Finbarr', 2, 4, 35), ('Gareth', 2, 4, 40), ('Helen', 2, 4, 45)
                                     ],
                                     my_schema
                                   )

    # 3. Operation T1: Window aggregation-based transformation to generate the ranking per country

    # 3.1. We create the window we want to aggregate by
    my_window = pyspark.sql.Window.partitionBy(inputDF["Val1"], inputDF["Val2"]).orderBy(inputDF["Val3"].asc())

    # 3.2. We apply the window aggregation-based transformation
    resultDF = inputDF.withColumn("prev_Val3", pyspark.sql.functions.lag(inputDF["Val3"]).over(my_window) )

    # 4. Operation A1: Action of displaying the content of resultDF
    resultDF.show()

# ------------------------------------------
# FUNCTION get_offset_row_value
# ------------------------------------------
def get_offset_row_value(spark, offset):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Val1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val2", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val3", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1, 10), ('Barry', 1, 1, 15), ('Connor', 1, 1, 20), ('Diarmuid', 1, 1, 25),
                                       ('Emma', 2, 4, 30), ('Finbarr', 2, 4, 35), ('Gareth', 2, 4, 40), ('Helen', 2, 4, 45)
                                     ],
                                     my_schema
                                   )

    # 3. Operation T1: Window aggregation-based transformation to generate the ranking per country

    # 3.1. We create the window we want to aggregate by
    my_window = pyspark.sql.Window.partitionBy(inputDF["Val1"], inputDF["Val2"]).orderBy(inputDF["Val3"].asc())

    # 3.2. We apply the window aggregation-based transformation
    resultDF = inputDF.withColumn("prev_Val3", pyspark.sql.functions.lag(inputDF["Val3"], offset, None).over(my_window) )

    # 4. Operation A1: Action of displaying the content of resultDF
    resultDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option, offset):
    if (option == 1):
        print("\n\n--- Lag to get previous row value ---\n\n")
        get_previous_row_value(spark)

    if (option == 2):
        print("\n\n--- Lag to get an offset row value ---\n\n")
        get_offset_row_value(spark, offset)

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
    offset = -2

    # 2. Local or Databricks
    pass

    # 3. We configure the Spark Context
    pass

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark, option, offset)
