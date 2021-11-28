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
import pyspark.sql.types
import pyspark.sql.functions

# ------------------------------------------
# FUNCTION replace_none_with_fixed_value
# ------------------------------------------
def replace_none_with_fixed_value(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, None), ('Bruce', None, True), ('Connor', None, None), (None, None, None) ], my_schema )

    # 3. Operation T1: We delete rows with any null value
    auxDF = inputDF.na.fill("", ["Name"])

    aux2DF = auxDF.na.fill(-1, ["Number"])

    resultDF = aux2DF.na.fill(False, ["LikesTennis"])

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()


# ------------------------------------------
# FUNCTION replace_none_with_average_value
# ------------------------------------------
def replace_none_with_average_value(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("LikesTennis", pyspark.sql.types.BooleanType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1, None), ('Bruce', 2, True), ('Connor', 5, None), ('Diarmuid', None, False), ('Eimeer', None, False) ], my_schema )

    # 3. Operation P1: We persist inputDF
    inputDF.persist()

    # 4. Operation T1: Compute avg value of "Number" in inputDF
    avgDF = inputDF.agg({"Number": "avg"}).withColumnRenamed("avg(Number)", "avg_val")

    # 5. Operation T2: We cast the avg value to an integer
    castDF = avgDF.withColumn("avg_val", avgDF["avg_val"].cast(pyspark.sql.types.IntegerType()))

    # 6. Operation T3: Cross join with inputDF
    joinDF = inputDF.crossJoin(castDF)

    # 7. Operation T4: We replace the NA in Number by a Dummy Value
    replacedDF = joinDF.na.fill(-1, ["Number"])

    # 8. Operation T5: We replace the NA
    goodDF = replacedDF.withColumn("FinalNumber", pyspark.sql.functions.when(replacedDF["Number"] == -1, replacedDF["avg_val"])
                                                                       .otherwise(replacedDF["Number"]))

    # 9. Operation T6: We tidy things up
    resultDF = goodDF.drop("avg_val")\
                     .drop("Number")\
                     .withColumnRenamed("FinalNumber", "Number")

    # 10. Operation P2: We persist resultDF
    resultDF.persist()

    # 11. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 12. Operation A2: Action of displaying the content of resultDF
    resultDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, option):
    if (option == 1):
        print("\n\n--- Replace None values by fixed values ---\n\n")
        replace_none_with_fixed_value(spark)

    if (option == 2):
        print("\n\n--- Replace None values by average values ---\n\n")
        replace_none_with_average_value(spark)


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
