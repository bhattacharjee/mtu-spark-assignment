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
            vehicle_id,
            day_picked,
            delay_limit
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C2: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    inputDF.createOrReplaceTempView('input_tbl')
    qry = """
                SELECT
                    busLineID as lineID,
                    closerStopID as stationID,
                    date_format(to_timestamp(date), 'HH:mm:ss') as arrivalTime,
                    %d as onTime,
                    atStop
                FROM
                    input_tbl
                WHERE
                    date like '{} %s'
                    AND
                    atStop = 1
                    AND
                    vehicleID = {}
                    AND
                    (delay %s {} %s delay %s {})
                ORDER BY
                    date, vehicleID, busLineID """\
            .format(day_picked, vehicle_id, -delay_limit, delay_limit)

    query1 = qry % (1, "%", ">=", "AND", "<=")
    query2 = qry % (0, "%", "<", "OR", ">")
    union_query = """
        SELECT
            *
        FROM
            (
            {}
            )
            UNION
            (
            {}
            )
        ORDER BY
            arrivalTime""".format(query1, query2)

    ws2 = Window.partitionBy('lineID', 'stationID').orderBy('arrivalTime')
    windowSpec = Window.partitionBy().orderBy('arrivalTime')
    solutionDF = spark\
                    .sql(union_query)\
                    .withColumn('rnum', f.row_number().over(windowSpec))\
                    .withColumn('lag', f.lag('rnum', default=-1).over(ws2))\
                    .where(f.col('rnum') - f.col('lag') != 1)\
                    .select(
                            f.col('lineID'),
                            f.col('stationID'),
                            f.col('arrivalTime'),
                            f.col('onTime')
                    )\
                    .orderBy('arrivalTime')\

    solutionDF.persist()
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
    vehicle_id = 33145
    day_picked = "2013-01-02"
    delay_limit = 60

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_1/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_3/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    #my_dataset_dir = '/home/phantom/3_Code_Examples/L07-23_Spark_Environment/FileStore/tables/6_Assignments/temp'
    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    my_main(spark, my_dataset_dir, vehicle_id, day_picked, delay_limit)

    total_time = time.time() - start_time
    #print("Total time = " + str(total_time))

