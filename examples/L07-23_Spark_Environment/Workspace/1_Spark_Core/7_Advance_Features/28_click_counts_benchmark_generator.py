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
import sys
import os
import shutil
import codecs
import random

# ------------------------------------------
# FUNCTION create_default_table_file
# ------------------------------------------
def create_default_table_file(my_folder):
    # 1. We open the input_stream
    my_input_stream = codecs.open(my_folder + "table.txt", "w", encoding='utf-8')

    # 2. We fill in the file content
    my_input_stream.write("1 1 3 5\n")
    my_input_stream.write("2 1 2\n")
    my_input_stream.write("3 2 5\n")
    my_input_stream.write("4 4\n")
    my_input_stream.write("5 3 4\n")

    # 3. We close the input_stream
    my_input_stream.close()

# ------------------------------------------
# FUNCTION create_default_time_step_files
# ------------------------------------------
def create_default_time_step_files(my_folder):
    # 1. We open the time_step_1
    my_input_stream = codecs.open(my_folder + "time_step_1.txt", "w", encoding='utf-8')

    # 2. We fill in the file content
    my_input_stream.write("1 1 2\n")
    my_input_stream.write("2 2\n")
    my_input_stream.write("3 4 5\n")

    # 3. We close the input_stream
    my_input_stream.close()

    # 4. We open the time_step_2
    my_input_stream = codecs.open(my_folder + "time_step_2.txt", "w", encoding='utf-8')

    # 5. We fill in the file content
    my_input_stream.write("1 1 3 5\n")
    my_input_stream.write("4 4\n")
    my_input_stream.write("5 1 2\n")

    # 6. We close the input_stream
    my_input_stream.close()

    # 7. We open the time_step_3
    my_input_stream = codecs.open(my_folder + "time_step_3.txt", "w", encoding='utf-8')

    # 8. We fill in the file content
    my_input_stream.write("4 4 5\n")
    my_input_stream.write("5 1 2 3\n")

    # 9. We close the input_stream
    my_input_stream.close()

# ------------------------------------------
# FUNCTION create_new_table_file
# ------------------------------------------
def create_new_table_file(my_folder, num_users, num_topics):
    # 1. We open the input_stream
    my_input_stream = codecs.open(my_folder + "table.txt", "w", encoding='utf-8')

    # 2. We fill all the lines of the file
    for user_id in range(num_users):
        # 2.1. Each line starts with the user_id
        my_input_stream.write(str(user_id + 1))

        # 2.2. We subscribe the user to a number of k topics
        k = random.randint(1, num_topics)

        # 2.3. The topics start from position p
        p = random.randint(1, num_topics)

        # 2.4. We fill in the topics
        for iteration in range(k):
            # 2.4.1. We subscribe the user to the topic
            my_input_stream.write(" " + str(p))

            # 2.4.2. We move on to the next topic
            if (p == num_topics):
                p = 0
            p = p + 1

        # 2.5. We write the end of line
        my_input_stream.write("\n")

    # 3. We close the input_stream
    my_input_stream.close()

# ------------------------------------------
# FUNCTION create_new_time_step_file
# ------------------------------------------
def create_new_time_step_file(my_folder,
                              file_index,
                              num_users,
                              num_topics,
                              time_steps_max_percentage_num_users,
                              time_steps_max_percentage_num_topics):
    # 1. We open the input_stream
    my_input_stream = codecs.open(my_folder + "time_step_" + str(file_index) + ".txt", "w", encoding='utf-8')

    # 2. We randomly decide the number of users subscribing to topics

    # 2.1. We calculate max_val based on the desired percentage
    max_val = int((num_users * time_steps_max_percentage_num_users) / 100.0)

    # 2.2. We check max_val is in the accepted bounds
    if (max_val == 0):
        max_val = 1
    if (max_val > num_users):
        max_val = num_users

    # 2.3. We apply max_val
    time_step_num_users = random.randint(1, max_val)

    # 3. We randomly decide the starting user
    user_id = random.randint(1, num_users)

    # 2. We fill all the lines of the file
    for iteration in range(time_step_num_users):
        # 2.1. Each line starts with the user_id
        my_input_stream.write(str(user_id))

        # 2.2. We subscribe the user to a number of k topics

        # 2.2.1. We calculate max_val based on the desired percentage
        max_val = int((num_topics * time_steps_max_percentage_num_topics) / 100.0)

        # 2.2.2.  We check max_val is in the accepted bounds
        if (max_val == 0):
            max_val = 1
        if (max_val > num_topics):
            max_val = num_topics

        # 2.2.3. We apply max_val
        k = random.randint(1, max_val)

        # 2.3. The topics start from position p
        p = random.randint(1, num_topics)

        # 2.4. We fill in the topics
        for iteration in range(k):
            # 2.4.1. We subscribe the user to the topic
            my_input_stream.write(" " + str(p))

            # 2.4.2. We move on to the next topic
            if (p == num_topics):
                p = 0
            p = p + 1

        # 2.5. We write the end of line
        my_input_stream.write("\n")

        # 2.6. We move in to the next user
        if (user_id == num_users):
            user_id = 0
        user_id = user_id + 1

    # 3. We close the input_stream
    my_input_stream.close()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(my_folder,
            new_benchmark,
            num_users,
            num_topics,
            num_time_steps,
            time_steps_max_percentage_num_users,
            time_steps_max_percentage_num_topics):

    # 1. We create an empty folder to start with
    if os.path.exists(my_folder):
        shutil.rmtree(my_folder)
    os.mkdir(my_folder)

    # 2. If we are creating the default benchmark
    if (new_benchmark == False):
        # 2.1. We create the default table file
        create_default_table_file(my_folder)

        # 2.2. We create the default time_step files
        create_default_time_step_files(my_folder)

    # 3. If we are creating a new benchmark
    else:
        # 3.1. We create the new table file
        create_new_table_file(my_folder, num_users, num_topics)

        # 3.2. We create the new time_step files
        for index in range(num_time_steps):
            create_new_time_step_file(my_folder,
                                      index + 1,
                                      num_users,
                                      num_topics,
                                      time_steps_max_percentage_num_users,
                                      time_steps_max_percentage_num_topics
                                     )

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
    # 1. We collect the input values

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        # I. We specify the folder name
        my_folder = sys.argv[1]

        # II. We specify whether we want the dafault benchmark (value 0) or a new one (value 1)
        new_benchmark = int(sys.argv[2])

        # III. We specify the number of users
        num_users = int(sys.argv[3])

        # IV. We specify the number of topics
        num_topics = int(sys.argv[4])

        # V. We specify the number of time steps
        num_time_steps = int(sys.argv[5])

        # VI. We specify the maximum number of users subscribing to new topics per time step
        time_steps_max_percentage_num_users = int(sys.argv[6])

        # VII. We specify the maximum number of topics each user can subscribe to per time step
        time_steps_max_percentage_num_topics = int(sys.argv[7])

    # 1.2. If we call the program from PyCharm then we hardcode the arguments to the values we want
    else:
        # I. We specify the folder name
        my_folder = "../../../FileStore/tables/1_Spark_Core/click_counts_dataset/"

        # II. We specify whether we want the dafault benchmark (value 0) or a new one (value 1)
        new_benchmark = 0

        # III. We specify the number of users
        num_users = 3250

        # IV. We specify the number of topics
        num_topics = 6500

        # V. We specify the number of time steps
        num_time_steps = 8

        # VI. We specify the maximum number of users subscribing to new topics per time step
        time_steps_max_percentage_num_users = 1

        # VII. We specify the maximum number of topics each user can subscribe to per time step
        time_steps_max_percentage_num_topics = 5

    # 2. We call to my_main
    my_main(my_folder,
            new_benchmark,
            num_users,
            num_topics,
            num_time_steps,
            time_steps_max_percentage_num_users,
            time_steps_max_percentage_num_topics
           )
