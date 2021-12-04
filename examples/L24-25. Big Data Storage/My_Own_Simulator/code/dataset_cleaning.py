#!/usr/local/bin/python3.8
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

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(input_file_name, output_file_name):
    # 1. We open the files for reading and writing
    my_input_stream = codecs.open(input_file_name, "r", encoding="utf-8")
    my_output_stream = codecs.open(output_file_name, "w", encoding="utf-8")

    # 2. We create the list of books
    book_list = []

    # 3. We create the info of each book
    category = ""
    title = " "
    author = " "

    # 3. We get the category being read
    for line in my_input_stream:
        # 3.1. We get the info from the line
        info = line.strip().split(" by ")

        # 3.2. If the line starts a new category of books, we update it
        if (len(info) == 1):
            category = info[0]

        # 3.3. Otherwise, we process the book
        else:
            # 3.3.1. We get the info from the line
            author = info[-1]
            del info[-1]
            title = "".join(info)

            # 3.2. We append the info
            my_str = title + ";" + author + ";" + category + "\n"
            book_list.append(my_str)

    # 4. We sort them by title
    book_list.sort()

    # 5. We start writing the book list in a random order
    size = len(book_list)
    while (size > 0):
        # 5.1. We pick an index randomly
        index = random.randint(0, size-1)

        # 5.2. We write it to the output
        my_output_stream.write(book_list[index])

        # 5.3. We remove it
        del book_list[index]

        # 5.4. We reduce the size
        size -= 1

    # 6. We close the files
    my_input_stream.close()
    my_output_stream.close()

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
    input_file_name = "../Dataset_Generation/Original_Book_Dataset.txt"
    output_file_name = "../Dataset_Generation/my_dataset.csv"

    # 4. We call to my_main
    my_main(input_file_name, output_file_name)
