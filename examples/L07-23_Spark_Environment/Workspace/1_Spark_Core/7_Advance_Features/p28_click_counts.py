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

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 1.1. We will output the user_id
    user_id = None

    # 1.2. We will output the list of topics the user is subscribed to
    user_topics = []

    # 2. We get the info from the line
    line = line.replace("\n", "")

    user_topics = line.split(" ")
    user_topics = list(map(int, user_topics))

    user_id = user_topics[0]
    del user_topics[0]

    # 3. We assign res
    res = (user_id, user_topics)

    # 4. We return res
    return res

# ------------------------------------------
# FUNCTION has_novel_click
# ------------------------------------------
def has_novel_click(my_pair):
    # 1. We generate the output value to return
    res = False

    # 2. We collect the elements
    user_topics = my_pair[1][0]
    user_clicks = my_pair[1][1]

    # 3. We traverse the clicks so as to see if there is any not being in user_topics
    size = len(user_clicks)
    index = 0
    while (index < size) and (res == False):
        # 3.1. If the click is new we finish
        if user_clicks[index] not in user_topics:
            res = True
        # 3.2. Otherwise, we continue with the next one
        else:
            index = index + 1

    # 4. We output res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, num_time_steps, include_partitions, num_partitions):
    # 1. Operation C1: We load user_table into
    inputRDD = sc.textFile(my_dataset_dir + "table.txt")

    # 2. Operation T1: We process each line of the file so as to have the format (user_id, user_topics)
    tableRDD = inputRDD.map(process_line)

    # 3. Operation P1: We specify the way tableRDD is partitioned
    if (include_partitions == True):
        tableRDD = tableRDD.partitionBy(num_partitions)

    # 4. Operation P2: We persist tableRDD
    tableRDD.persist()

    # 5. Main Loop
    step = 1

    while (step <= num_time_steps):
        # 5.1. We print the current time step
        print("\n\n\n--- Time Step = " + str(step))

        # 5.2. Operation C2: We load the next time step
        input_stepRDD = sc.textFile(my_dataset_dir + "time_step_" + str(step) + ".txt")

        # 5.3. Operation T2: We process each line of the file so as to have the format (user_id, user_topics)
        table_stepRDD = input_stepRDD.map(process_line)

        # 5.4. Operation T3: We join tableRDD and table_stepRDD
        joinRDD = tableRDD.join(table_stepRDD)

        # 5.5. Operation T4: We filter the ones with novel clicks
        usersWithNovelClicksRDD = joinRDD.filter(has_novel_click)

        # 5.6. Operation A1: We count how many users are there
        resVAL = usersWithNovelClicksRDD.count()

        # 5.7. We display this info
        print(resVAL)

        # 5.8. We join tableRDD and table_stepRDD
        step = step + 1

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
    num_time_steps = 8
    include_partitions = False
    num_partitions = 8

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/1_Spark_Core/click_counts_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, num_time_steps, include_partitions, num_partitions)
