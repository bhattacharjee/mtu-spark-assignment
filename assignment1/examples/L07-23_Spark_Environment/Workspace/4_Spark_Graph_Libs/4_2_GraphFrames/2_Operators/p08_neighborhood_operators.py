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
import graphframes
import graphframes.lib

# ------------------------------------------
# FUNCTION my_aggregate_messages
# ------------------------------------------
def my_aggregate_messages(spark, my_dataset_dir):
    # 1. We load my_edgesDF from the dataset

    # 1.1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
      [pyspark.sql.types.StructField("src", pyspark.sql.types.IntegerType(), True),
       pyspark.sql.types.StructField("dst", pyspark.sql.types.StringType(), True)
       ])

    # 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    my_edges_datasetDF = spark.read.format("csv") \
      .option("delimiter", " ") \
      .option("quote", "") \
      .option("header", "false") \
      .schema(my_schema) \
      .load(my_dataset_dir)

    # 1.3. Operation T1: We add the value column
    my_edgesDF = my_edges_datasetDF.withColumn("value", pyspark.sql.functions.lit(1))

    # 1.4. Operation P1: We persist it
    my_edgesDF.persist()

    # 1.5. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    my_verticesDF = my_edgesDF.select(my_edgesDF["src"]) \
      .withColumnRenamed("src", "id") \
      .dropDuplicates() \
      .withColumn("value", pyspark.sql.functions.lit(1))

    # 1.6. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    myGF = graphframes.GraphFrame(my_verticesDF, my_edgesDF)

    # 2. Operation T4: We aggregate messages for myGF. In particular, for each v we aggregate the sum of all neighbour

    # 2.1. We create the aggregate object, which provides a kind of triplet view: src, edge, dst
    myAO = graphframes.lib.AggregateMessages()

    # 2.2. Operation T4: We aggregate the messages in a DataFrame
    # (1) The first parameter is the merge function: How do we aggregate all messages being sent?
    #     In this case it just uses the function sum to sum them all
    # (2) The second argument is the sendToSrc messages: Given each element of the triplets view: What message do we send per edge?
    #     In this case we just decide to send the id of the destination vertex
    # (3) The third argument is the sendToDst messages: Given each element of the triplets view: What message do we send per edge?
    #     In this case we just decide to send the id of the source vertex
    my_aggregatedDF = myGF.aggregateMessages(pyspark.sql.functions.sum(myAO.msg).alias("result"),
                                 myAO.dst["id"],
                                 myAO.src["id"],
                                )

    # 3. Operation A1: We display the content of my_aggregatedDF
    my_aggregatedDF.show()

# ------------------------------------------
# FUNCTION my_collect_neighbor_ids
# ------------------------------------------
def my_collect_neighbor_ids(spark, my_dataset_dir, direction):
    # 1. We load my_edgesDF from the dataset

    # 1.1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("src", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("dst", pyspark.sql.types.StringType(), True)
        ])

    # 1.2. Operation C1: We create the DataFrame from the dataset and the schema
    my_edges_datasetDF = spark.read.format("csv") \
        .option("delimiter", " ") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 1.3. Operation T1: We add the value column
    my_edgesDF = my_edges_datasetDF.withColumn("value", pyspark.sql.functions.lit(1))

    # 1.4. Operation P1: We persist it
    my_edgesDF.persist()

    # 1.5. Operation T3: We transform my_edgesDF so as to get my_verticesDF
    my_verticesDF = my_edgesDF.select(my_edgesDF["src"]) \
        .withColumnRenamed("src", "id") \
        .dropDuplicates() \
        .withColumn("value", pyspark.sql.functions.lit(1))

    # 1.6. Operation C3: We create myGF from my_verticesDF and my_edgesDF
    myGF = graphframes.GraphFrame(my_verticesDF, my_edgesDF)

    # 2. Operation T4: We get the edges again
    my_edgesDF = None
    if (direction == True):
          my_edgesDF = myGF.edges.drop("value") \
                                 .groupBy("src") \
                                 .agg(pyspark.sql.functions.collect_list("dst").alias("out_neighbors"))
    else:
          my_edgesDF = myGF.edges.drop("value") \
                                 .groupBy("dst") \
                                 .agg(pyspark.sql.functions.collect_list("src").alias("in_neighbors"))

    # 3. Operation A1: We display my_edgesDF
    my_edgesDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option, direction):
    # We decide which function to call to based on the option
    if (option == 1):
        my_aggregate_messages(spark, my_dataset_dir)
    elif (option == 2):
        my_collect_neighbor_ids(spark, my_dataset_dir, direction)

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
    direction = False

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/4_Spark_Graph_Libs/1_TinyGraph/my_dataset"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, option, direction)
