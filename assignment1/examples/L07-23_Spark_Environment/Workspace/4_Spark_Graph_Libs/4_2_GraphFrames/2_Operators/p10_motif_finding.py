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

from pyspark.sql.functions import col, lit, when

# ------------------------------------------
# FUNCTION motif_finding_stateless
# ------------------------------------------
def motif_finding_stateless(spark, my_dataset_dir, query):
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

    # 2. Operation P2: We persist myGF
    myGF.persist()

    # 3. Next, we perform some queries, showing the resultDF

    if (query == 1):
        # 3.1.1 Operation T4: A cycle among 3 vertices
        my_resultDF1 = myGF.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")

        # 3.1.2: Operation A1: We show the solution
        my_resultDF1.show()

    elif (query == 2):
        # 3.2.1 Operation T5: A transitivity relation with no cycle
        my_resultDF2 = myGF.find("(a)-[e1]->(b); (b)-[e2]->(c); !(c)-[]->(a); !(a)-[]->(c)")

        # 3.2.2: Operation A2: We show the solution
        my_resultDF2.show()

    elif (query == 3):
        # 3.3.1 Operation T6: Vertices c with in-degree at least two and out-degree at least one
        #                     Vertices d with out-degree at least two and in-degree at least one
        #                     With a connection from c to d, but not from d to c

        find_c_auxDF = myGF.find("(a)-[e1]->(c); (b)-[e2]->(c)")
        find_c_aux2DF = find_c_auxDF.filter(find_c_auxDF["a"] != find_c_auxDF["b"])
        find_c_DF = find_c_aux2DF.drop_duplicates(["c"]).withColumnRenamed("c", "c1")

        find_d_auxDF = myGF.find("(d)-[e3]->(e); (d)-[e4]->(f)")
        find_d_aux2DF = find_d_auxDF.filter(find_d_auxDF["e"] != find_d_auxDF["f"])
        find_d_DF = find_d_aux2DF.drop_duplicates(["d"]).withColumnRenamed("d", "d1")

        find_connections_DF = myGF.find("(c)-[e5]->(d); !(d)-[]->(c)")
        join1_DF = find_connections_DF.join(find_c_DF, find_connections_DF["c"] == find_c_DF["c1"], "inner")
        join2_DF = join1_DF.join(find_d_DF, join1_DF["d"] == find_d_DF["d1"], "inner")

        my_resultDF3 = join2_DF.select(join2_DF["c"], join2_DF["d"])

        # 3.3.2: Operation A2: We show the solution
        my_resultDF3.show()

# ------------------------------------------
# FUNCTION my_fold
# ------------------------------------------
def my_fold(funct, my_list, accum):
    # 1. We create the output variable
    res = accum

    # 2. We populate the list with the higher application
    for item in my_list:
        res = res + funct(accum, item)

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION motif_finding_stateful
# ------------------------------------------
def motif_finding_stateful(spark, my_dataset_dir):
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

    # 2. Operation P2: We persist myGF
    myGF.persist()

    # 3. We perform our stateful query

    # 3.1. Operation T4: We find our pattern
    my_patternDF = myGF.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")

    # 3.2. We define our state_update function, to be applied for each element in the motif
    my_state_update = lambda accum, vertex_id: when(vertex_id % 2 == 1, accum + 1).otherwise(accum)

    # 3.3. We define our state_computation to the elements of the sequence
    my_state_generate = my_fold( lambda accum, v: my_state_update(accum, col(v).id), ["a", "b", "c"], lit(0) )

    # 3.4. Operation T5: We apply our state_computation function to my_patternDF
    my_resultDF = my_patternDF.where(my_state_generate >= 2)

    # 3.5. Operation A1: We show my_resultDF
    my_resultDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option, query):
    # We decide which function to call to based on the option
    if (option == 1):
        motif_finding_stateless(spark, my_dataset_dir, query)
    elif (option == 2):
        motif_finding_stateful(spark, my_dataset_dir)

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
      query = 3

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
      my_main(spark, my_dataset_dir, option, query)
