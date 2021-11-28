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
import shutil
import os

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, longitude, num_lines):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.

    # The dataset was previously loaded into DBFS by:
    # i) Place the desired dataset into your local file system folder %Test Environment%/my_dataset/
    # ii) Running the script "1.upload_dataset.bat"

    # Please note that, once again, the name textFile is a false friend here, as it seems that the parameter we are passing is the name of the file
    # we want to read. Indeed, the parameter here is the name of a folder, and the dataset consists on the content of the files of the folder.
    # An RDD of Strings is then generated, with one item per line of text in the files.
    # In this case, the RDD is distributed among different machines (nodes) of our cluster.

    #                            C1: textFile
    # dataset: DBFS inputFolder -------------> inputRDD

    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: Transformation 'filter', so as to get a new RDD ('filteredRDD') with lines having at list length 'longitude'.

    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: filter
    #                        |------------> filteredRDD     --- RDD items are String ---

    filteredRDD = inputRDD.filter(lambda x: len(x) >= longitude)

    # 3. Operation A1: Action 'take', so as to take only a subset of the items of the RDD.

    # It returns n elements from the RDD and attempts to minimize the number of partitions it accesses, so it may represent a biased collection.
    # It's important to note that these operations do not return the elements in the order you might expect.


    #         C1: textFile
    # dataset -------------> inputRDD      --- RDD items are String ---
    #                        |
    #                        | T1: filter
    #                        |------------> filteredRDD     --- RDD items are String ---
    #                                       |
    #                                       | A1: take
    #                                       | ---------> resVAL     --- Iterator of items String ---

    # This example is fantastic to explain what lazy evaluation is about.
    # In this example, both inputRDD and filteredRDD are not computed when 'declared' in lines 32 and 42, resp.
    # The Spark driver evaluates this program and says:
    # Uhm, I clearly see 4 pieces of information here: [dataset, inputRDD, fiteredRDD, resVAL]
    #                                                    DATA       RDD        RDD      VALUE
    # Interestingly, when inputRDD was declared, nobody was requiring to check its internal state, so nothing was computed.
    # Same story for filteredRDD, when it was declared nobody was requiring to check its internal state, so nothing was computed.

    # Now, when running the Action A1: take(2), it is requesting 2 lines from filteredRDD to compute resVAL,
    # so I'm sorry but filteredRDD has to be computed now.
    # That said, does resVAL requires filteredRDD to be fully computed? Not at all.
    # So we can think of resVAL and filteredRDD having A1: take(2) as their protocol of communication.
    # In other words, two adults having a conversation to try to negotiate and reach a common agreement.
    #  The communication from resVAL and filteredRDD (via the protocol A1: take(2)) can be as follows:
    # "filteredRDD, please give me two lines, this is more than enough for me" - says resVAL.
    # "Ok, perfect" - replies filteredRDD.
    # As you see, the agreement here was easy :)

    # Now let's move backwards to filteredRDD.
    # It has to communicate with inputRDD. In this case, their protocol of communication is filter.
    # So, once again, here we are in front of 2 adults having a conversation to try to negotiate and reach a common agreement.
    #  The communication from filteredRDD and inputRDD (via the protocol T1: filter) can be as follows:

    # "inputRDD, according to filter, I will need you to be fully computed" - filteredRDD will say.
    # "Oh no, really?" - replies the lazy inputRDD -.
    # "Uhm, no, I also have some good news from resVAL, apparently he only needs 2 elements from me,
    # so that means I only need 2 elements from you as well" - filteredRDD continues -.
    # "Oh, this is great news! So, does this mean I only need to compute 2 elements myself" - replies inputRDD, happy as a kid
    # on her/his birthday.

    # "No, unfortunately I cannot guarantee you that" -filteredRDD might reply-, "maybe 2 is enough, but maybe you will need
    # to compute 3 items, maybe 4, 5, ... or 1 million. Indeed, you will need to compute as many as needed, until 2
    # of these items satisfies our protocol of communication filter".
    # "Oh, I see" -says inputRDD- "What a pity!".

    # "I know, but let's do something. Let's prceed as follows. You compute yourself one line at a time.
    # Right after getting a new line you stop, and you come back to me, and we both ask our protocol filter if this line (item)
    # passes the test or not. If it does, I check if this is the last line I really wanted, and if so, we're done.
    # And, until this happens, we keep repeating this loop over and over again with one new line (item) at a time.
    # Does it sound ok?"

    # "Yes, it sound much better than computing myself entirely" - inputRDD replies-.
    # "I know, lazy evaluation is great! Do you think all this story will make SDH4 students to understand lazy evaluation?
    # or they might not even bother in reading this?" -filteredRDD wonders-.
    # "I don't know, it's a great story, so I give a 50%-50% chance. But, anyway, why do we care? We are only RDDs, so this
    # is not our business" -inputRDD finishes-.

    # So let's end the story and move backwards to inputRDD.
    # It has to communicate with dataset. In this case, their protocol of communication is textFile.
    # So, once again, here we are in front of 2 adults having a conversation to try to negotiate and reach a common agreement.
    #  The communication from inputRDD and dataset (via the protocol C1: textFile) can be as follows:

    # "dataset, according to textFile, I will need you to pass me all your lines" - inputRDD will say.
    # "Oh no, really?" - replies the lazy dataset -.
    # "Uhm, no, I also have some kind of agreement with filteredRDD, apparently he only needs me to compute the lines 1 by 1
    # until it is satisfied.
    # So let's take advantage of this and have our own way to proceed here:
    # I will ask you 1 line at a time. I might come back to you multiple times, requesting a new line, until either filteredRDD
    # gets its 2 lines, or you give me the entire dataset, whatever happens first.
    # Once I don't need more lines from you, I will let you know. Does it sound ok?"

    # "Yes, it sound much better than doing the effort of passing you the entire dataset in one go" - dataset replies-.
    # "I know, lazy evaluation is great! Do you think..."
    # "Oh, please, stop, don't go again with this story again. Perfect, we have reach an agreement, so our job is done."
    # -dataset finishes-.

    # And this is how lazy evaluation works!

    resVAL = filteredRDD.take(num_lines)

    # 4. We print by the screen the collection computed in resVAL
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
    longitude = 20
    num_lines = 2

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/1_Spark_Core/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, longitude, num_lines)
