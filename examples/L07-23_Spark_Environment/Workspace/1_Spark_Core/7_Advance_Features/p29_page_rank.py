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
# FUNCTION my_filter_function
# ------------------------------------------
def my_filter_function(pair):
    # 1. We generate the output value to return
    res = False

    # 2. We collect the elements
    l1 = pair[1][0]
    l2 = pair[1][1]

    # 3. We traverse the elements of l2, to see if any is novel w.r.t. the content of l1
    size = len(l2) - 1
    while (size >= 0) and (res == False):
        # 3.1. If the elements was used, then we keep trying with the next element
        if l2[size] in l1:
            size = size - 1
        # 3.2. If the element is novel, we set the pair to pass the filter
        else:
            res = True

    # 4. We output res
    return res


# ------------------------------------------
# FUNCTION count_users_clicking_new_links
# ------------------------------------------
def count_users_clicking_new_links(sc):
    # bigRDD = sc.parallelize([(1,[1,2,3]), (2,[4,5]), (3,[1,3,5])])

    # Much better, as this bigRDD will not be suffled for each join operation
    bigRDD = sc.parallelize([(1, [1, 2, 3]), (2, [4, 5]), (3, [1, 3, 5])]).partitionBy(3).persist()

    # Whereas user 1 didn't click in new links, users 2 and 3 did.
    smallRDD = sc.parallelize([(4, [6]), (1, [2, 3]), (2, [3, 4]), (3, [1, 3, 4])])

    # 1. We make the inner join of both tables
    innerJoinRDD = bigRDD.join(smallRDD)
    for w in innerJoinRDD.collect():
        print("(" + str(w[0]) + ", " + str(w[1]) + ")")
    print("")

    # 2. We filter the desired users
    filteredUsersRDD = innerJoinRDD.filter(my_filter_function)
    for w in filteredUsersRDD.collect():
        print("(" + str(w[0]) + ", " + str(w[1]) + ")")
    print("")

    # 3. We count the amount of users that passed the filter
    num_users = filteredUsersRDD.count()
    print(num_users)


# ------------------------------------------
# FUNCTION my_partitioner_policy
# ------------------------------------------
def my_partitioner_policy(key):
    res = ord(key) % 2
    return res


# ------------------------------------------
# FUNCTION my_contribution
# ------------------------------------------
def my_contribution(key_value_tuple):
    # 1. We create the variable to return
    res = []

    # 2. We collect the info from the pair
    neighbours = key_value_tuple[1][0]
    rank = key_value_tuple[1][1]

    # 3. We populate the list of contributions
    size = len(neighbours)
    for n in neighbours:
        res.append((n, rank / size))

    # 4. We output the result variable
    return res


# ------------------------------------------
# FUNCTION page_rank
# ------------------------------------------
def page_rank(sc, n):
    # 1. We load the pages info
    linksRDD = sc.parallelize([("A", ["B", "D", "E"]),
                               ("B", ["A", "C", "E", "F"]),
                               ("C", ["B", "F"]),
                               ("D", ["A"]),
                               ("E", ["A", "B", "F", "G", "H"]),
                               ("F", ["B", "C", "E"]),
                               ("G", ["E", "H"]),
                               ("H", ["E", "G"])
                               # ]).partitionBy(num_partitions, my_partitioner_policy).persist()
                               ]).partitionBy(n).persist()

    # 2. We initialise the ranks
    ranksRDD = linksRDD.mapValues(lambda x: 1.0)

    # 3. We run the iterations n times
    for i in range(n):
        # 3.1 We join both tables --> (pageID, ([links], rank))
        joinRDD = linksRDD.join(ranksRDD)

        # 3.2. We generate a single list with all the contributions
        contributionsRDD = joinRDD.flatMap(my_contribution)

        # 3.3. We reduce by key, so as to get the total contribution each element receives
        totalContributionsRDD = contributionsRDD.reduceByKey(lambda x, y: x + y)

        # 3.4 We update ranksRDD
        ranksRDD = totalContributionsRDD.mapValues(lambda x: 0.15 + (0.85 * x))

        # 4. We display the results
    for w in ranksRDD.collect():
        print("(" + str(w[0]) + ", " + str(w[1]) + ")")
    print("")


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, n):
    print("--- count_users_clicking_new_links ---")
    count_users_clicking_new_links(sc)

    print("--- page_rank ---")
    page_rank(sc, n)

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
    n = 30

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 3. We call to my_main
    my_main(sc, n)
