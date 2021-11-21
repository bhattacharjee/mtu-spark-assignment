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
def my_main(                                                                \
            spark,                                                          \
            my_dataset_dir,                                                 \
            month_picked,                                                   \
            delay_limit=60*5,                                               \
            late_count_threshold=100,                                       \
            late_percentage_threshold=50                                    \
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
    def is_late_fn(x):
        return 1 if x > delay_limit else 0
    is_late = f.udf(is_late_fn, pyspark.sql.types.IntegerType())

    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    inputDF = inputDF\
        .filter(month_picked == f.substring(f.col('date'), 0, 7))\
        .where(f.col('atStop') == 1)\
        .withColumn('time', f.substring(f.col('date'), 12, 9))\
        .withColumn('isLate', is_late(f.col('delay')))\
        .drop('congestion', 'longitude', 'latitude', 'busLinePatternumID')\
        .withColumn('hour', f.substring(f.col('time'), 0, 2))

    inputDF.persist()

    # If a bus has been late at a stop, count it, but count every instance only once
    # Do not count consecutive late instances
    ws2 = Window.partitionBy('vehicleID', 'closerStopID', 'isLate').orderBy('vehicleId', 'time', 'closerStopID')
    windowSpec = Window.partitionBy().orderBy('vehicleId', 'time')
    uniqueLateDF = inputDF\
                    .where(f.col('isLate') == 1)\
                    .withColumn('rnum', f.row_number().over(windowSpec))\
                    .withColumn('lag', f.lag('rnum', default=-1).over(ws2))\
                    .where(f.col('rnum') - f.col('lag') != 1)

    aggregatedLateDF = uniqueLateDF\
                    .groupBy(['busLineID', 'hour'])\
                    .count()

    # If a bus has been at a top once, count it only once
    ws2 = Window.partitionBy('vehicleID', 'closerStopID').orderBy('vehicleId', 'time')
    windowSpec = Window.partitionBy().orderBy('vehicleId', 'time')
    uniqueAtStopDF = inputDF.withColumn('rnum', f.row_number().over(windowSpec))\
                    .withColumn('lag', f.lag('rnum', default=-1).over(ws2))\
                    .where(f.col('rnum') - f.col('lag') != 1)
    aggregatedAtStopDF = uniqueAtStopDF\
                    .groupBy(['busLineID', 'hour'])\
                    .count()

    inputDF.unpersist()
    aggregatedLateDF.persist()
    aggregatedAtStopDF.persist()

    aggregatedLateDF.createOrReplaceTempView('late_instances')
    aggregatedAtStopDF.createOrReplaceTempView('all_instances')
    inputDF.unpersist()
    aggregatedLateDF.persist()
    aggregatedAtStopDF.persist()


    query = """
        SELECT
            ROUND(late_instances.count / all_instances.count * 100, 2) AS percentage,
            late_instances.hour AS hour,
            late_instances.busLineID AS busLineID
        FROM
            late_instances
        INNER JOIN
            all_instances
        ON
            late_instances.hour = all_instances.hour
            AND
            late_instances.busLineID = all_instances.busLineID
            AND
            late_instances.count > {}
            AND
            (late_instances.count * 100 / all_instances.count) > {}
        ORDER BY
            percentage
        DESC
    """.format(late_count_threshold, late_percentage_threshold)

    solutionDF = spark.sql(query)
    aggregatedLateDF.unpersist()
    aggregatedAtStopDF.unpersist()
    solutionDF.persist()

    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    count = 0
    for item in resVAL:
        count = count + 1
        print(f"{item}")

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
    # TO BE COMPLETED
    month_picked = "2013-01"
    delay_limit_to_consider_late = 60 * 5 # 15 minutes delay
    late_count_below_which_to_ignore = 100
    late_percentage_below_which_to_ignore = 10.0

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex5_micro_dataset_1/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    #my_dataset_dir = '/home/phantom/3_Code_Examples/L07-23_Spark_Environment/FileStore/tables/6_Assignments/temp'
    # 4. We configure Spark
    # TO BE COMPLETED
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    # TO BE COMPLETED
    my_main(                                                                \
            spark,                                                          \
            my_dataset_dir,                                                 \
            month_picked,                                                   \
            delay_limit=delay_limit_to_consider_late,                       \
            late_count_threshold=late_count_below_which_to_ignore,          \
            late_percentage_threshold=late_percentage_below_which_to_ignore)

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
