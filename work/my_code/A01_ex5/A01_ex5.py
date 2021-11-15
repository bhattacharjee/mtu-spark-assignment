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
            delay_limit=60                                                  \
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
    def get_month(s):
        return s[:7]
    spark.udf.register('GET_MONTH', get_month, pyspark.sql.types.StringType())

    def get_time(s):
        return s[11:]
    spark.udf.register('GET_TIME', get_time, pyspark.sql.types.StringType())

    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)\
        
    inputDF.persist()
    inputDF.createOrReplaceTempView('base_table')

    #inputDF = inputDF\
    #            .where(f.substring(f.col('date'), 0, 7) == month_picked)

    inputDF = spark.sql("""
        (
            SELECT
                GET_MONTH(date), GET_TIME(date)
            FROM
                base_table
        )
    """)


    #ws2 = Window.partitionBy('vehicleID').orderBy('date')
    #windowSpec = Window.partitionBy().orderBy('date')

    #numberedDF = inputDF.withColumn('rnum', f.row_number().over(windowSpec))\
    #                .withColumn('lag', f.lag('rnum', default=-1).over(ws2))
    #                .where(f.col('rnum') - f.col('lag') != 1)

    for l in inputDF.take(500):
        print(l)

    # TO BE COMPLETED
    for i in numberedDF.take(50):
        print(i)
    pass

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
    # TO BE COMPLETED
    month_picked = "2013-01"

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

    my_dataset_dir = '/home/phantom/3_Code_Examples/L07-23_Spark_Environment/FileStore/tables/6_Assignments/temp'
    # 4. We configure Spark
    # TO BE COMPLETED
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    # TO BE COMPLETED
    my_main(                                                                \
            spark,                                                          \
            my_dataset_dir,                                                 \
            month_picked)

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
