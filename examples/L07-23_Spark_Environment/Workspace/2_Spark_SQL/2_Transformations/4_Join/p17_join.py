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
# FUNCTION inner_single_column
# ------------------------------------------
def inner_single_column(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the inner join based in one column
    resultDF = input1_DF.join(input2_DF,
                              input1_DF["A_Number"] == input2_DF["B_Number"],
                              "inner"
                             )

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 8. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION inner_multiple_columns
# ------------------------------------------
def inner_multiple_columns(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the inner join based in a list of columns
    resultDF = input1_DF.join(input2_DF,
                              [input1_DF["A_Number"] == input2_DF["B_Number"],
                               input1_DF["A_Name"] == input2_DF["B_Name"]
                              ],
                              "inner"
                              )

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 8. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION left_outer_single_column
# ------------------------------------------
def left_outer_single_column(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the left-outer join based in one column
    resultDF = input1_DF.join(input2_DF, input1_DF["A_Number"] == input2_DF["B_Number"], "left_outer")

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 8. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION left_anti_single_column
# ------------------------------------------
def left_anti_single_column(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the left-anti join based in one column
    resultDF = input1_DF.join(input2_DF, input1_DF["A_Number"] == input2_DF["B_Number"], "left_anti")

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 8. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION full_outer_single_column
# ------------------------------------------
def full_outer_single_column(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the full-outer join based in one column
    resultDF = input1_DF.join(input2_DF, input1_DF["A_Number"] == input2_DF["B_Number"], "full_outer")

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 8. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION full_outer_minus_inner
# ------------------------------------------
def full_outer_minus_inner(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the left-anti join based in one column
    aux1_DF = input1_DF.join(input2_DF, input1_DF["A_Number"] == input2_DF["B_Number"], "left_anti")

    # 6. Operation T2: We get the right-anti join based in one column
    aux2_DF = input2_DF.join(input1_DF, input1_DF["A_Number"] == input2_DF["B_Number"], "left_anti")

    # 7. Operation T3: We get the full-outer of aux1DF and aux2DF
    resultDF = aux1_DF.join(aux2_DF, aux1_DF["A_Number"] == aux2_DF["B_Number"], "full_outer")

    # 8. Operation P1: We persist resultDF
    resultDF.persist()

    # 9. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 10. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION cross_join
# ------------------------------------------
def cross_join(spark):
    # 1. We define the Schema of our DF1.
    my_schema1 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("A_Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("A_Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation CreateDataFrame
    input1_DF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema1 )

    # 3. We define the Schema of our DF2.
    my_schema2 = pyspark.sql.types.StructType([pyspark.sql.types.StructField("B_Number", pyspark.sql.types.IntegerType(), True),
                                               pyspark.sql.types.StructField("B_Name", pyspark.sql.types.StringType(), True)
                                              ]
                                             )

    # 4. Operation C2: Creation CreateDataFrame
    input2_DF = spark.createDataFrame( [ (3, 'Eileen'), (4, 'Diarmuid'), (5, 'Gareth'), (6, 'Helen') ], my_schema2 )

    # 5. Operation T1: We get the full-outer join based in one column
    resultDF = input1_DF.crossJoin(input2_DF)

    # 6. Operation P1: We persist resultDF
    resultDF.persist()

    # 7. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 8. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option):
    if (option == 1):
        print("\n\n--- Inner join based in 1 column ---\n\n")
        inner_single_column(spark)

    if (option == 2):
        print("\n\n--- Inner join based in multiple columns ---\n\n")
        inner_multiple_columns(spark)

    if (option == 3):
        print("\n\n--- Left outer join based in 1 column ---\n\n")
        left_outer_single_column(spark)

    if (option == 4):
        print("\n\n--- Left-anti based in 1 column ---\n\n")
        left_anti_single_column(spark)

    if (option == 5):
        print("\n\n--- Full outer join based in 1 column ---\n\n")
        full_outer_single_column(spark)

    if (option == 6):
        print("\n\n--- Full outer minus inner based in 1 column ---\n\n")
        full_outer_minus_inner(spark)

    if (option == 7):
        print("\n\n--- Cross Join (Cartesian Product) ---\n\n")
        cross_join(spark)


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
    option = 1

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
