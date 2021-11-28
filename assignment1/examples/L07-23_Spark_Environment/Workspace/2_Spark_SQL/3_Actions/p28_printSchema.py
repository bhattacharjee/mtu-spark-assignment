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
# FUNCTION my_main
# ------------------------------------------
def my_main(spark):
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

    # 2. Operation C1: Creation 'createDataFrame', so as to store the content of the people into a DataFrame (DF).
    inputDF = spark.createDataFrame([p1, p2, p3, p4, p5])

    # 3. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()


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
    pass

    # 2. Local or Databricks
    pass

    # 3. We configure the Spark Context
    pass

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We run my_main
    my_main(spark)
