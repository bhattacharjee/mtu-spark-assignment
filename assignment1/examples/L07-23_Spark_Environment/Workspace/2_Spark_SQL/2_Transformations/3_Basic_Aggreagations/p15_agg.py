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
# FUNCTION agg_single_column
# ------------------------------------------
def agg_single_column(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Val1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val2", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val3", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1, 10), ('Barry', 1, 1, 15), ('Connor', 1, 3, 20),
                                       ('Diarmuid', 2, 4, 25), ('Emma', 2, 4, 30), ('Finbarr', 2, 6, 35) ], my_schema )

    # 3. Operation P1: We persist inputDF
    inputDF.persist()

    # 4. We run different aggregations based on a single column

    # 4.1. Operation T1: Agg to get the count
    resultDF1 = inputDF.agg( {"Val3" : "count"} )

    # 4.2 Operation A1: Action show
    print("\n--- count Results ---\n")
    resultDF1.show()

    # 4.3. Operation T2: Agg to get the count
    resultDF2 = inputDF.agg( {"Val3" : "count"} )\
                       .withColumnRenamed("count(Val3)", "count")

    # 4.4 Operation A2: Action show
    print("\n--- count Results with Alias ---\n")
    resultDF2.show()

    # 4.5. Operation T3: Agg to get the avg
    resultDF3 = inputDF.agg( {"Val3" : "avg"} )\
                       .withColumnRenamed("avg(Val3)", "avg")

    # 4.6 Operation A3: Action show
    print("\n--- avg Results with Alias ---\n")
    resultDF3.show()

    # 4.7. Operation T4: Agg to get the max
    resultDF4 = inputDF.agg( {"Val3" : "max"} )\
                       .withColumnRenamed("max(Val3)", "max")

    # 4.8 Operation A4: Action show
    print("\n--- max Results with Alias ---\n")
    resultDF4.show()

    # 4.9. Operation T5: Agg to get the min
    resultDF5 = inputDF.agg( {"Val3" : "min"} )\
                       .withColumnRenamed("min(Val3)", "min")

    # 4.10 Operation A5: Action show
    print("\n--- min Results with Alias ---\n")
    resultDF5.show()

    # 4.11. Operation T6: Agg to get the sum
    resultDF6 = inputDF.agg( {"Val3" : "sum"} )\
                       .withColumnRenamed("sum(Val3)", "sum")

    # 4.12 Operation A6: Action show
    print("\n--- sum Results with Alias ---\n")
    resultDF6.show()

# ------------------------------------------
# FUNCTION agg_multiple_columns
# ------------------------------------------
def agg_multiple_columns(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Val1", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val2", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("Val3", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, 1, 10), ('Barry', 1, 1, 15), ('Connor', 1, 3, 20),
                                       ('Diarmuid', 2, 4, 25), ('Emma', 2, 4, 30), ('Finbarr', 2, 6, 35) ], my_schema )

    # 3. Operation P1: We persist inputDF
    inputDF.persist()

    # 4. We run different aggregations based on multiple columns

    # 4.1. Operation T1: Agg to get the count
    resultDF1 = inputDF.agg( {"Val3" : "count", "Val1" : "sum"} )

    # 4.2 Operation A1: Action show
    print("\n--- count Results ---\n")
    resultDF1.show()

    # 4.3. Operation T2: Agg to get the avg
    resultDF2 = inputDF.agg( {"Val3" : "avg", "Val1" : "sum"} )

    # 4.4 Operation A2: Action show
    print("\n--- avg Results ---\n")
    resultDF2.show()

    # 4.5. Operation T3: Agg to get the max
    resultDF3 = inputDF.agg( {"Val3" : "max", "Val1" : "sum"} )

    # 4.6 Operation A3: Action show
    print("\n--- max Results ---\n")
    resultDF3.show()

    # 4.7. Operation T4: Agg to get the min
    resultDF4 = inputDF.agg( {"Val3" : "min", "Val1" : "sum"} )

    # 4.8 Operation A4: Action show
    print("\n--- min Results ---\n")
    resultDF4.show()

    # 4.9. Operation T5: Agg to get the sum
    resultDF5 = inputDF.agg( {"Val3" : "sum", "Val1" : "sum"} )

    # 4.10 Operation A5: Action show
    print("\n--- sum Results ---\n")
    resultDF5.show()


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option):
    if (option == 1):
        print("\n\n--- Aggregate single column ---\n\n")
        agg_single_column(spark)

    if (option == 2):
        print("\n\n--- Aggregate multiple columns ---\n\n")
        agg_multiple_columns(spark)

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
