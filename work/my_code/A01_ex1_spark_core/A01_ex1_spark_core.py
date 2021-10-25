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
from datetime import datetime

# ORIGINAL SCHEMA
# ---------------
#         0                 1             2         3          4          5        6          7         8        9
#       DATE             BUS_LINE   BUS_LINE_PTN  CONGEST   LOINGITD   LATITUD   DELAY     VEHICL   CLSR_STP   AT_STP
# '2013-01-19 08:00:36',   40,       '015B1002',    0,     -6.258078, 53.359279,  300,      33488,      279,     0
# ---------------

# ORIGINAL SCHEMA
# ---------------
#         0                 1             2         3          4          5        6          7         8        9        10           11
#       DATE             BUS_LINE   BUS_LINE_PTN  CONGEST   LOINGITD   LATITUD   DELAY     VEHICL   CLSR_STP   AT_STP   HOUROFDAY DAYOFWEEK
# '2013-01-19 08:00:36',   40,       '015B1002',    0,     -6.258078, 53.359279,  300,      33488,      279,     0        12            5
# ---------------

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




# Add two elements to each row
def enriched_line(line):
    raw_tuple = process_line(line)
    dateformat = '%Y-%m-%d %H:%M:%S'
    d = datetime.strptime(raw_tuple[0], dateformat)
    date_info_tuple = (d.hour, d.weekday())
    return raw_tuple + date_info_tuple




def filter_by_latitude_longitude(lat1:float, lat2: float, long1: float, long2:float):
    latitude1 = min(lat1, lat2)
    latitude2 = max(lat1, lat2)
    longitude1 = min(long1, long2)
    longitude2 = max(long1, long2)

    def filter_fn(row: tuple)->bool:
        the_latitude = row[5]
        the_longitude = row[4]
        the_congestion = row[3]
        if the_latitude <= latitude2 and the_latitude >= latitude1 and \
            the_longitude <= longitude2 and the_longitude >= longitude1:
            return True
        else:
            return False

    return filter_fn




def filter_by_weekday_latitude_longitude_and_hours(\
        lat1:float,\
        lat2:float,\
        long1:float,\
        long2:float,\
        hours_list:list):

    latitude1 = min(lat1, lat2)
    latitude2 = max(lat1, lat2)
    longitude1 = min(long1, long2)
    longitude2 = max(long1, long2)
    hours = hours_list if hours_list != None else list(range(0, 13))
    hours = [int(h) for h in hours]

    def filter_fn(row: tuple)->bool:
        the_latitude = row[5]
        the_longitude = row[4]
        the_congestion = row[3]
        return is_weekday(row) and get_hour(row) in hours and \
            the_latitude <= latitude2 and the_latitude >= latitude1 and \
            the_longitude <= longitude2 and the_longitude >= longitude1

    return filter_fn



def is_weekday(row):
    dateformat = '%Y-%m-%d %H:%M:%S'
    d = datetime.strptime(row[0], dateformat)
    return d.weekday() < 5

def key_value_by_hour(row):
    dateformat = '%Y-%m-%d %H:%M:%S'
    d = datetime.strptime(row[0], dateformat)
    return (d.hour, row)

def get_hour(row):
    dateformat = '%Y-%m-%d %H:%M:%S'
    d = datetime.strptime(row[0], dateformat)
    return d.hour

def get_percentage(x):
    try:
        return x[0] * 100 / x[1]
    except:
        return 0.0

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            north,
            east,
            south,
            west,
            hours_list
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------

    # Split by the key and create a tuple
    splitRDD = inputRDD.map(process_line)

    # Filter by coordinates, weekday and hours
    filteredRDD = splitRDD.filter(\
            filter_by_weekday_latitude_longitude_and_hours(\
                north, south, east, west, hours_list))

    # Now that we have the filtered RDD, we will create a new RDD that contains 3 things:
    # 1. hour 
    # 2.  PAIR
    #     2.1 Count of congested (0 or 1)
    #     2.2 Total count (always 1)
    pairedRDD = filteredRDD.map(\
            lambda x: (get_hour(x), (x[3], 1,)))

    # We need to find aggregates per hour, so we'll create a pair
    # where the hour is the key
    reducedRDD = pairedRDD.reduceByKey(\
            lambda x, y: (x[0] + y[0], x[1] + y[1],))

    # groupedRDD contains the percentages and counts
    groupedRDD = reducedRDD.map(\
                    lambda x: \
                    (f'{x[0]:02d}', get_percentage(x[1]), x[1][1], x[1][0]))

    # Now we need to sort it, so we'll need to create another pair
    # where the percentage is the key, since we are sorting by percentage
    pairedRDD = groupedRDD.map(lambda x: (x[1], x))

    # After sorting, remove the key from the pair
    solutionRDD = pairedRDD.sortByKey(ascending=False).map(lambda x: x[1])

    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionRDD.collect()
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
    north = 53.3702027
    east = -6.2043634
    south = 53.3343619
    west = -6.2886331
    hours_list = ["07", "08", "09"]

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    #my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex1_micro_dataset_1/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex1_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex1_micro_dataset_3/"
    my_dataset_dir = '/home/phantom/nacho_assignment/data/my_dataset_complete'
    #my_dataset_dir = '/home/phantom/nacho_assignment/data/A01_ex1_micro_dataset_1'

    if local_False_databricks_True == False:
        # TODO: get this back to normal
        #my_dataset_dir = my_local_path + my_dataset_dir
        my_dataset_dir = my_dataset_dir
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
            north,
            east,
            south,
            west,
            hours_list
           )

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
