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

def get_sortable(x: tuple):
    return (x[7], x[0],)

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
                        0)))            # Lenth of segment

    def join_segments(first: tuple, second: tuple) -> tuple:
        if first[1] > second[0]:
            print(f"Unexpected overlaps between segments {first} {second}")
            return first 
        if first[0] > first[1] or second[0] > second[1]:
            print(f"Segments not properly formed {first} {second}")
            return first 

        # Swap if first comes after second
        if first[0] > second[0]:
            first, second = second, first


        return first 
        

    reducedRDD = pairedRDD.reduceByKey(join_segments)


    def prdd(xx, name):
        print(name)
        print('------------------------------')
        print(day_picked)
        print('----------')
        [print(x) for x in xx.collect()]
        print('=' * 80, '\n\n\n')

    prdd(parsedRDD, 'parsedRDD')
    prdd(reducedRDD, 'sortedRDD')




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
    bucket_size = 50
    max_speed_accepted = 28.0
    day_picked = "2013-01-07"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_local_path = ""
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_1/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex4_micro_dataset_3/"
    my_dataset_dir = "/home/phantom/nacho_assignment/data/A01_ex4_micro_dataset_1"
    #my_dataset_dir = "/home/phantom/nacho_assignment/data/my_dataset_complete"

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
    print("Total time = " + str(total_time))
