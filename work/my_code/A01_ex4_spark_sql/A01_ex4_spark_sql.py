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
import time
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternumID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    filteredDF = \
        inputDF.select \
        (
            f.col('date'),
            f.col('vehicleID'),
            f.col('latitude'),
            f.col('longitude')
        ).where \
        ( \
            f.col('date').like(f"{day_picked}%") \
        )
    
    windowSpec = Window.partitionBy('vehicleID').orderBy('date')
    numberedDF = filteredDF.withColumn('rnum', f.row_number().over(windowSpec))
    
    numberedDF.createOrReplaceTempView('base_tbl')

    join_query = """
        SELECT
            A.vehicleID     AS vehicleID,
            A.date          AS date1,
            A.latitude      AS latitude1,
            A.longitude     AS longitude1,
            B.date          AS date2,
            B.latitude      AS latitude2,
            B.longitude     AS longitude2
        FROM
            base_tbl A,
            base_tbl B
        WHERE
            A.vehicleID = B.vehicleID AND
            A.rnum + 1 = B.rnum"""

    joinedDF = spark.sql(join_query)



    [print(r) for r in d.collect()]

    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)

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
    bucket_size = 50
    max_speed_accepted = 28.0
    day_picked = "2013-01-07"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_1/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_3/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    my_main(spark,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
           )

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
