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

# -----------------------------------------------
# FUNCTION createDataFrame_Rows_implicit_schema
# -----------------------------------------------
def createDataFrame_Rows_implicit_schema(spark):
    # 1. We create the Row objects
    p1 = pyspark.sql.Row(identifier=pyspark.sql.Row(name="Luis", surname="Johnson"),
                         eyes="Brown",
                         city="Madrid",
                         age=34,
                         likes=[pyspark.sql.Row(sport="Football", score=10),
                                pyspark.sql.Row(sport="Basketball", score=6),
                                pyspark.sql.Row(sport="Tennis", score=5),
                               ]
                         )

    p2 = pyspark.sql.Row(identifier=pyspark.sql.Row(name="John", surname="Rossi"),
                         eyes="Blue",
                         city="Paris",
                         age=20,
                         likes=[pyspark.sql.Row(sport="Football", score=8),
                                pyspark.sql.Row(sport="Basketball", score=4)
                               ]
                         )

    p3 = pyspark.sql.Row(identifier=pyspark.sql.Row(name="Francesca", surname="Depardieu"),
                         eyes="Blue",
                         city="London",
                         age=26,
                         likes=[pyspark.sql.Row(sport="Tennis", score=8),
                                pyspark.sql.Row(sport="Basketball", score=7)
                               ]
                         )

    p4 = pyspark.sql.Row(identifier=pyspark.sql.Row(name="Laurant", surname="Muller"),
                         eyes="Green",
                         city="Paris",
                         age=26,
                         likes=[pyspark.sql.Row(sport="Tennis", score=1)
                               ]
                         )

    p5 = pyspark.sql.Row(identifier=pyspark.sql.Row(name="Gertrud", surname="Gonzalez"),
                         eyes="Green",
                         city="Dublin",
                         age=32,
                         likes=[pyspark.sql.Row(sport="Rugby", score=10)
                               ]
                         )

    # 2. Operation C1: Creation 'createDataFrame'.
    inputDF = spark.createDataFrame([p1, p2, p3, p4, p5])

    # 3. Operation P1: Persistance of the DF my_rawDF, as we are going to use it three times.
    inputDF.persist()

    # 4. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()

    # 5. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
    inputDF.show()

    # 6. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
    newDF = inputDF.withColumn("new_age", pyspark.sql.functions.col("age") + 1)

    # 7. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
    newDF.show()

# ---------------------------------------------
# FUNCTION createDataFrame_RDD_explicit_shema
# ---------------------------------------------
def createDataFrame_RDD_explicit_shema(sc, spark):
    # 1. Operation C1: Creation 'parallelize'
    inputRDD = sc.parallelize([('Madrid', 34), ('Paris', 20), ('London', 26), ('Paris', 26), ('Dublin', 32)])

    # 2. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("city", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("age", pyspark.sql.types.IntegerType(), True)
                                             ]
                                            )

    # 3. Operation C2: Creation 'createDataFrame'
    inputDF = spark.createDataFrame(inputRDD, my_schema)

    # 4. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()

    # 5. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
    inputDF.show()

    # 6. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
    newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 7. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
    newDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, spark, option):
    if (option == 1):
        print("\n\n--- DataFrame from Row Objects and Implicit Schema ---\n\n")
        createDataFrame_Rows_implicit_schema(spark)

    if (option == 2):
        print("\n\n--- DataFrame from RDD and Explicit Schema ---\n\n")
        createDataFrame_RDD_explicit_shema(sc, spark)

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
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(sc, spark, option)

