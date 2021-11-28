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
import sys
import os

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sol_name, my_result_dir):
    # 1. We create the solutionRDD
    res = []

    # 2. We collect all the files of the dir
    instances = os.listdir(my_result_dir)

    # 3. We filter-out the instances with .crc extension
    valid_instances = [name for name in instances if name[-4:] != ".crc" ]

    # 3. We collect the content of each file
    for name in valid_instances:
        # 3.1. We check that the file

        # 3.1. We open the file
        my_input_file = codecs.open(my_result_dir + name, "r", encoding='utf-8')

        # 3.2. We read it line by line
        for line in my_input_file:
            res.append(line)

        # 3.3. We close the input file
        my_input_file.close()

    # 4. We open the file for writing
    my_output_file = codecs.open(my_result_dir + sol_name, "w", encoding='utf-8')

    # 5. We write the content to the file
    for line in res:
        my_output_file.write(line)

    # 6. We close the output file
    my_output_file.close()

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
    # 1. We collect the arguments
    sol_name = "solution.txt"
    my_result_dir = "./my_result/"

    if len(sys.argv) > 1:
        sol_name = sys.argv[1]
        my_result_dir = sys.argv[2]

    # 2. We call to my_main
    my_main(sol_name, my_result_dir)
