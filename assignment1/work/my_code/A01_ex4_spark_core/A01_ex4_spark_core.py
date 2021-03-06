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
import datetime
from math import radians, cos, sin, asin, sqrt
from haversine import haversine

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    #(00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    #(01) Bus_Line => The bus line. Int (e.g., 120).
    #(02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    #(03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    #(04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    #(05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    #(06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    #(07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    #(08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    #(09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 10):
        # 3.1. We set the right type for the parameters
        params_list[1] = int(params_list[1])
        params_list[3] = int(params_list[3])
        params_list[4] = float(params_list[4])
        params_list[5] = float(params_list[5])
        params_list[6] = int(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[8] = int(params_list[8])
        params_list[9] = int(params_list[9])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res

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

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    
    # Select lines for the day we have
    selectedRDD = inputRDD.filter(lambda x: x.startswith(day_picked))
    selectedRDD.persist()

    # Parse the lines
    parsedRDD = selectedRDD.map(process_line)

    # sort by vehicle id and timestamp
    sortedRDD = parsedRDD.sortBy(ascending=True, keyfunc=lambda x: (x[7], x[0],))

    # Create a map, each segment is a set of coordinates along the route
    # of a vehicle. We just store the first and the last coordinate
    # This will allow us to merge two consecutive coordinates
    pairedRDD = sortedRDD.map( \
            lambda x: \
            (
                x[7],   # vehicle id
                ( \
                        x[0],           # start timestamp for segment
                        x[0],           # end timestamp for segment
                        (x[4], x[5]),   # start coordinates for segment
                        (x[4], x[5]),   # end coordinates for segments
                        0               # Lenth of segment
                )
            ))
    selectedRDD.unpersist()
    pairedRDD.persist()

    # >>----------------------------------------------------
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
    # <<----------------------------------------------------

    reducedRDD = pairedRDD.reduceByKey(join_segments) \
                            .map(lambda x: x[1][4])

    # >>----------------------------------------------------
    def get_initial_bucket(x: float):
        bucket_num = int(x // bucket_size)
        bucket_range = f"{int(bucket_num * bucket_size)}_{int(bucket_num * bucket_size + bucket_size)}"
        return (bucket_num, (bucket_num, bucket_range, 1))
    # <<----------------------------------------------------

    initialBucketRDD = reducedRDD.map(get_initial_bucket)
    solutionRDD = initialBucketRDD.reduceByKey(lambda x, y: (x[0], x[1], x[2] + y[2])) \
                                    .map(lambda x: x[1]) \
                                    .sortBy(lambda x: x[0])

    pairedRDD.unpersist()
    solutionRDD.persist()
    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionRDD.collect()
    for item in resVAL:
        print(item)

    # ---------------------------------------

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
    #my_dataset_dir = "/home/phantom/nacho_assignment/data/A01_ex4_micro_dataset_1"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    my_main(sc,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
           )

    total_time = time.time() - start_time
    #print("Total time = " + str(total_time))
