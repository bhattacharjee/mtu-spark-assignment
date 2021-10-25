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

import codecs
import random
import sys
import os

# ------------------------------------------
# FUNCTION merge_files
# ------------------------------------------
def merge_files(sol_name, dir_name):
    # 1. We create the variable to output
    res = False

    # 2. We create the solutionRDD
    content = []

    # 3. We collect all the files of the dir
    files = os.listdir(dir_name)

    # 4. We collect the content of each file
    for file_name in files:
        if file_name[-4:] != ".crc":
            # 4.1. We open the file
            my_input_file = codecs.open(dir_name + file_name, "r", encoding='utf-8')

            # 4.2. We read it line by line
            for line in my_input_file:
                content.append(line)

            # 4.3. We close the input file
            my_input_file.close()

    # 5. We avoid empty time_steps: If any actual line has been read at all
    if (len(content) > 0):
        # 5.1. We open the file for writing
        my_output_file = codecs.open(dir_name + sol_name, "w", encoding='utf-8')

        # 5.2. We write the content to the file
        for line in content:
            my_output_file.write(line)

        # 5.3. We close the output file
        my_output_file.close()

        # 5.4. We increase res
        res = True

    # 6. We return res
    return res

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sol_name, my_result_dir):
    # 1. We create a list with the directories we want to rename
    directories_to_rename = []

    # 2. We collect all the sub-directories names (there is one sub-directory per time_interval)
    directories = os.listdir(my_result_dir)

    directories.sort()

    # 3. We traverse each sub-directory (time_interval)
    for dir in directories:
        # 3.1. We get the name of the sub-directory
        dir_name = my_result_dir + "/" + dir + "/"

        # 3.2. We merge the parts of such sub-directory
        valid = merge_files(sol_name, dir_name)

        # # 3.3. If the directory is valid (contains some results), we append its name to res
        if valid:
            directories_to_rename.append(str(my_result_dir) + "/" + str(dir))

    # 4. We traverse the directories so as to rename them
    time_step = 1
    for item in directories_to_rename:
        # 4.1. We rename the directory name
        os.rename(item, item + " (time_step " + str(time_step) + ")")

        #4.2. We increase the time step for renaming next directory
        time_step = time_step + 1


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
    # 1. If we run it from command line
    if len(sys.argv) > 1:
        my_main(sys.argv[1], sys.argv[2])
    # 2. If we run it via Pycharm
    else:
        sol_name = "solution.txt"
        my_result_dir = "./my_result"
        my_main(sol_name, my_result_dir)
