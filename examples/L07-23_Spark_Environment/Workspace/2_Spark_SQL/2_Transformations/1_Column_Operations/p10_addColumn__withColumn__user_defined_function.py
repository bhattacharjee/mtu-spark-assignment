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
import pyspark.sql.types

# ------------------------------------------
# FUNCTION add_column_with_implicit_udf
# ------------------------------------------
def add_column_with_implicit_udf(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema )

    # 3. Operation T1: Transformation to create a new column

    # 3.1. We define the UDF function we will use to do the filtering
    my_power_lengthUDF = pyspark.sql.functions.udf(lambda x, y: len(x) ** y, pyspark.sql.types.IntegerType())

    # 3.2. We use the UDF to apply it to each Row of inputDF
    resultDF = inputDF.withColumn("NewListOfNumbers", my_power_lengthUDF(pyspark.sql.functions.col("Name"),
                                                                         pyspark.sql.functions.col("Number")
                                                                        )
                                 )

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()

# ------------------------------------------
# FUNCTION my_power_length
# ------------------------------------------
def my_power_length(name, num):
    # 1. We create the output variable
    res = len(name) ** num

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION add_column_with_explicit_udf
# ------------------------------------------
def add_column_with_explicit_udf(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema )

    # 3. Operation T1: Transformation to create a new column

    # 3.1. We define the UDF function we will use to do the filtering
    my_power_lengthUDF = pyspark.sql.functions.udf(my_power_length, pyspark.sql.types.IntegerType())

    # 3.2. We use the UDF to apply it to each Row of inputDF
    resultDF = inputDF.withColumn("NewListOfNumbers", my_power_lengthUDF(inputDF["Name"], inputDF["Number"]))

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
        print("\n\n--- Add column with implicit User defined Function (UDF) ---\n\n")
        add_column_with_implicit_udf(spark)

    if (option == 2):
        print("\n\n--- Add column with explicit User defined Function (UDF) ---\n\n")
        add_column_with_explicit_udf(spark)

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
