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
# FUNCTION create_new_column
# ------------------------------------------
def create_new_column(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema )

    inputDF.persist()

    inputDF.show()

    # 3. Operation T1: Transformation to create a new column
    resultDF = inputDF.withColumn("NewNumber", pyspark.sql.functions.col("Number") + 1)

    # 4. Operation P1: We persist resultDF
    resultDF.persist()

    # 5. Operation A1: Action to show the schema of resultDF
    resultDF.printSchema()

    # 6. Operation A2: Action of displaying the content of resultDF
    resultDF.show()

# ------------------------------------------
# FUNCTION replace_existing_column
# ------------------------------------------
def replace_existing_column(spark):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("Name", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("Number", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 2. Operation C1: Creation createDataFrame
    inputDF = spark.createDataFrame( [ ('Anna', 1), ('Bruce', 2), ('Connor', 3), ('Diarmuid', 4) ], my_schema )

    # 3. Operation T1: Transformation to create a new column
    resultDF = inputDF.withColumn("Number", pyspark.sql.functions.col("Number") + 1)

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
        print("\n\n--- Create new column ---\n\n")
        create_new_column(spark)

    if (option == 2):
        print("\n\n--- Replace existing column ---\n\n")
        replace_existing_column(spark)

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
