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

#exec(open("fp_features.py").read())

# ------------------------------------------
#
# 1. POLYMORPHISM
#
# ------------------------------------------
#
# ------------------------------------------
# FUNCTION fst
# ------------------------------------------
def fst(x, y):
    return x

# ------------------------------------------
# FUNCTION snd
# ------------------------------------------
def snd(x, y):
    return y

# ------------------------------------------
# FUNCTION my_take
# ------------------------------------------
def my_take(n, my_list):
    # 1. We create the output variable
    res = []

    # 2. We also get the length of the list
    size = len(my_list)

    # 3. We traverse as many elements as requested
    index = 0
    while (index < size):
        # 3.1. If the index is to be taken
        if (index < n):
            # 3.1.1. We append it to the result
            res.append(my_list[index])
            # 3.1.2. We continue by exploring the next index
            index = index + 1
        # 3.2. If the index is not to be taken
        else:
            # 3.2.1. We conclude the loop
            index = size

    # 4. We return res
    return res

# ------------------------------------------
#
# 2. HIGHER ORDER FUNCTIONS
#
# ------------------------------------------

# ------------------------------------------
# FUNCTION add_one
# ------------------------------------------
def add_one(x):
    # 1. We create the variable to output
    res = x + 1

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION my_add
# ------------------------------------------
def my_add(x, y):
    # 1. We create the variable to output
    res = x + y

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION my_mult
# ------------------------------------------
def my_mult(x, y):
    # 1. We create the variable to output
    res = x * y

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION bigger_than_three
# ------------------------------------------
def bigger_than_three(x):
    # 1. We create the variable to output
    res = False

    # 2. If the element is bigger than 3 we make it to pass the test
    if (x > 3):
        res = True

    # 2. We return res
    return res

# ------------------------------------------
# FUNCTION my_bigger
# ------------------------------------------
def my_bigger(x, y):
    # 1. We create the variable to output
    res = False

    # 2. If the element is bigger than 3 we make it to pass the test
    if (x < y):
        res = True

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_equal
# ------------------------------------------
def my_equal(x, y):
    # 1. We create the variable to output
    res = False

    # 2. If the element is bigger than 3 we make it to pass the test
    if (x == y):
        res = True

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(funct, my_list):
    # 1. We create the output variable
    res = []

    # 2. We populate the list with the higher application
    for item in my_list:
        sol = funct(item)
        res.append(sol)

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_filter
# ------------------------------------------
def my_filter(funct, my_list):
    # 1. We create the output variable
    res = []

    # 2. We populate the list with the higher application
    for item in my_list:
        # 2.1. If an item satisfies the function, then it passes the filter
        if funct(item) == True:
            res.append(item)

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION my_fold
# ------------------------------------------
def my_fold(funct, accum, my_list):
    # 1. We create the output variable
    res = accum

    # 2. We populate the list with the higher application
    for item in my_list:
        res = res + funct(accum, item)

    # 3. We return res
    return res

# ------------------------------------------
#
# 3. PARTIAL APPLICATION
#
# ------------------------------------------

# SAME FUNCTIONS AS ABOVE

# ------------------------------------------
#
# 4. LAZY EVALUATION
#
# ------------------------------------------

# ------------------------------------------
# FUNCTION loop
# ------------------------------------------
def loop():
    # 1. We create the variable to output
    res = loop()


    # res = 0
    #
    # #while (res == 0):
    # #    pass
    #
    # time.sleep()

    # 2. We return loop
    return res

# ------------------------------------------
# FUNCTION from_n
# ------------------------------------------
def from_n(n):
    # 1. We create the variable to output
    res = [n]

    # 2. We recursively compute res
    res = res + from_n(n+1)

    # 3. We return res
    return res

# ------------------------------------------
# FUNCTION n_first_primes
# ------------------------------------------
def n_first_primes(n):
    # 1. We generate the variable to output
    res = []

    # 2. We use a couple of extra variables
    candidate = 2
    primes_found = 0

    # 3. We foundd the first n prime numbers
    while (primes_found < n):

        # 3.1. We traverse all numbers in [2..(candidate-1)] to find if there is any divisor
        index = 2
        divisor_found = False

        while (index < candidate) and (divisor_found == False):
            # 3.1.1. If the number is a divisor, we stop
            if (candidate % index) == 0:
                divisor_found = True
            # 3.1.2. If it is not, we continue with the next number
            else:
                index = index + 1

        # 3.2. If the number is a prime
        if divisor_found == False:
            # 3.2.1. We include the prime number in the list
            res.append(candidate)
            # 3.2.2. We increase the number of prime numbers being found
            primes_found = primes_found + 1

        # 3.3. We increase candidate, to test the next integer as prime number
        candidate = candidate + 1

    # 4. We return res
    return res
