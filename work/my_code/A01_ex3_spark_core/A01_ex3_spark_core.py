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

def get_end_time(current_time:str, seconds_horizon:int) -> str:
    dateformat = "%Y-%m-%d %H:%M:%S"
    d = datetime.datetime.strptime(current_time, dateformat)
    d = d + datetime.timedelta(seconds=seconds_horizon)
    return d.strftime(dateformat)

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            current_time,
            seconds_horizon
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    
    # String comparisons for date will work here and will
    # be less expensive than parsing the date each time
    end_time = get_end_time(current_time, seconds_horizon)

    # Convert the string of lines to tuples
    parsedRDD = inputRDD.map(process_line)

    # Filter the records to include only
    # 1. Buses that are on the stop
    # 2. within the time window 
    filteredRDD = parsedRDD.filter( \
            lambda x: \
                1 == x[9] and \
                x[0] >= current_time and x[0] < end_time)

    pairedRDD = filteredRDD.map( \
            lambda x: \
                ( \
                    (x[8], x[7]),       # (closer-stop, vehicle-id)
                    (x[0], x[8], x[7])  # (timestamp, closer-stop, vehicle-id)
                ))
    
    # Drop entries where the same bus is in the same stop and has
    # been already counted
    uniqueVehicleStopRDD = pairedRDD.reduceByKey(lambda x, _: x)

    # Now we want to create a list like this (stop, [vehicle1, vehicle2]) and so on
    pairedRDD = uniqueVehicleStopRDD.map( \
            lambda x: \
                ( \
                    x[1][1],    # closer-stop
                    [x[1][2]]   # [vehicleid]
                ))
                 
    candidateRDD = pairedRDD.reduceByKey(lambda x, y: x + y)

    maxLength = len(candidateRDD.max(lambda x: len(x[1]))[1])

    filteredCandidateRDD = candidateRDD.filter(lambda x: maxLength == len(x[1]))

    solutionRDD = filteredCandidateRDD\
                    .map(lambda x: (x[0], sorted(x[1]))) \
                    .sortBy(lambda x: x[0])


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
    current_time = "2013-01-07 06:30:00"
    seconds_horizon = 1800

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex3_micro_dataset_1/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex3_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex3_micro_dataset_3/"

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

    my_main(sc, my_dataset_dir, current_time, seconds_horizon)

    total_time = time.time() - start_time
    #print("Total time = " + str(total_time))
