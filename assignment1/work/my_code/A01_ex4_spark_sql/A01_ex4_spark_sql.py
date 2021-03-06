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
from math import radians, cos, sin, asin, sqrt
import sys
import datetime
from haversine import haversine

def haversine_distance_internal(cord1: tuple, cord2: tuple) -> float:
    longitude1, latitude1 = cord1
    longitude2, latitude2 = cord2
    radius_of_earth_km = 6371.0088
    delta_lat_radians = radians(latitude2 - latitude1)
    delta_long_radians = radians(longitude2 - longitude1)
    latitude1 = radians(latitude1)
    latitude2 = radians(latitude2)
    a = sin(delta_lat_radians / 2) ** 2 
    a = a + cos(latitude1) * cos(latitude2) * sin(delta_long_radians / 2) ** 2
    c = 2 * asin(sqrt(a))
    return radius_of_earth_km * c

def haversine_distance(coord1: tuple, coord2: tuple) -> float:
    (lon1, lat1) = coord1
    (lon2, lat2) = coord2
    try:
        dist = haversine((lat1, lon1), (lat2, lon2))
        return dist
    except:
        return haversine_distance_internal(coord1, coord2)

def get_speed(ts1: str, ts2: str, dist:float) -> float:
    try:
        dateformat = "%Y-%m-%d %H:%M:%S"
        t1 = datetime.datetime.strptime(ts1, dateformat)
        t2 = datetime.datetime.strptime(ts2, dateformat)
        tt = t2 - t1
        seconds = abs(tt.total_seconds())
        return abs(dist) / seconds * 1000
    except:
        return sys.float_info.max

def join_segments(first: tuple, second: tuple) -> tuple:
    if first[1] > second[0]:
        #print(f"Unexpected overlaps between segments {first} {second}")
        return first 
    if first[0] > first[1] or second[0] > second[1]:
        #print(f"Segments not properly formed {first} {second}")
        return first 

    # Swap if first comes after second
    if first[0] > second[0]:
        first, second = second, first

    add_distance = haversine_distance(first[3], second[2])
    speed = get_speed(first[1], second[0], add_distance)
    add_distance = 0 if speed > max_speed_accepted else add_distance

    return (first[0],  \
            second[1], \
            first[2],  \
            second[3], \
            first[4] + second[4] + add_distance,)



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


    # --------------------------------------------------------------------
    def get_distance(ts1, lat1, lon1, ts2, lat2, lon2):
        try:
            distance = haversine_distance((lon1, lat1,), (lon2, lat2,))
            speed = get_speed(ts1, ts2, distance)
            if (speed > max_speed_accepted):
                return 0.0
            return distance
        except Exception as e:
            return 0.0

    dist_udf = f.udf(get_distance, pyspark.sql.types.FloatType())
    # --------------------------------------------------------------------


    # --------------------------------------------------------------------
    def get_bucketnum(dist):
        return int(dist // bucket_size)

    bucketnum_udf = f.udf(get_bucketnum, pyspark.sql.types.IntegerType())
    # --------------------------------------------------------------------


    # --------------------------------------------------------------------
    def get_bucket_range_str(bucket_num):
        return f"{int(bucket_num * bucket_size)}_{int(bucket_num * bucket_size + bucket_size)}"

    bucketrang_udf = f.udf(get_bucket_range_str, pyspark.sql.types.StringType())
    # --------------------------------------------------------------------


    # Filter only the rows for the date we are looking at
    # Select only columns:
    # date, vehicleID, latitude, longitude
    filteredDF =                                                            \
        inputDF.select                                                      \
        (                                                                   \
            f.col('date'),                                                  \
            f.col('vehicleID'),                                             \
            f.col('latitude'),                                              \
            f.col('longitude')                                              \
        ).where                                                             \
        (                                                                   \
            f.col('date').like(f"{day_picked}%")                            \
        )
    
    # Group by vehicleID and number the rows for each vehicle's entries
    # Add the column 'rnum' , this will be used later to calculate the distance
    # between consecutive rows
    windowSpec = Window.partitionBy('vehicleID').orderBy('date')
    numberedDF = filteredDF.withColumn('rnum', f.row_number().over(windowSpec))
    
    numberedDF.persist()
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
            A.rnum + 1 = B.rnum
        """

    # Cross with itself and join based on two criteria:
    #
    # vehicleID1 = vehicleID2
    # rnum1 + 1 = rnum2
    #
    # The second condition ensures only consecutive rows are joined
    joinedDF = spark.sql(join_query)


    # Calculate the distance and add it to the table
    # After that keep only two columns:
    # vehicleID, distance
    distanceDF = joinedDF                                                   \
                    .withColumn                                             \
                    (                                                       \
                        'distance',                                         \
                        dist_udf                                            \
                        (                                                   \
                            'date1', 'latitude1', 'longitude1',             \
                            'date2', 'latitude2', 'longitude2',             \
                        )                                                   \
                    )                                                       \
                    .select                                                 \
                    (                                                       \
                        f.col('vehicleID'),                                 \
                        f.col('distance')                                   \
                    )

    # Sum the distances to find the total distance for each vehicle
    aggregatedDF = distanceDF                                               \
                        .groupBy('vehicleID')                               \
                        .agg({'distance': 'sum'})                           \
                        .withColumnRenamed('sum(distance)', 'distance')

    
    # Convert the distance to the bucket number
    # The find the count of each bucket number to form the final histogram
    solutionDF = bucketizedDF = aggregatedDF \
                    .withColumn('bucket_id', bucketnum_udf('distance'))     \
                    .groupBy('bucket_id')                                   \
                    .agg({'vehicleID': 'count'})                            \
                    .withColumnRenamed('count(vehicleID)', 'num_vehicles')  \
                    .withColumn('bucket_size', bucketrang_udf('bucket_id')) \
                    .select                                                 \
                    (                                                       \
                        f.col('bucket_id'),                                 \
                        f.col('bucket_size'),                               \
                        f.col('num_vehicles')                               \
                    )                                                       \
                    .orderBy('bucket_id')

    solutionDF.persist()
    numberedDF.unpersist()

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
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_1/"
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
    #print("Total time = " + str(total_time))
