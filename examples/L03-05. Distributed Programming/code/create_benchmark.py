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
import codecs
import random
import os
import shutil

# ------------------------------------------
# FUNCTION generate_file
# ------------------------------------------
def generate_file(num_movies, file_name):
    # 1. We open the file to write
    my_input_file = codecs.open(file_name, "w", encoding='utf-8')

    # 2. We create the list
    num_list = [ (value + 1) for value in range(num_movies) ]

    # 3. We fill the file with the list content
    for iteration in range(num_movies):
        # 3.1. We pick a element from the list
        index = random.randint(0, num_movies-1)

        # 3.2. We print the item
        my_input_file.write(str(num_list[index]) + "\n")

        # 3.3. We delete the item
        del num_list[index]

        # 3.4. We decrease the number of movies
        num_movies = num_movies - 1

    # 4. We close the file
    my_input_file.close()

# ------------------------------------------
# FUNCTION generate_benchmark
# ------------------------------------------
def generate_benchmark(directory_name, num_people, num_movies):
    # 1. If the directory already contained some files, we remove them
    if os.path.exists(directory_name):
        #os.remove(directory_name)
        shutil.rmtree(directory_name)
        os.mkdir(directory_name)

    # 2. We generate the benchmark by creating the desired number of files
    for index in range(num_people):
        generate_file(num_movies, directory_name + "file_" + str(index + 1) + ".txt")
