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
# FUNCTION my_main
# ------------------------------------------
def my_main(sc):
    # 1. Operation C1: Creation 'parallelize'
    inputRDD  = sc.parallelize( [ "Hello", "Hola", "Bonjour", "Hello", "Bonjour", "Ciao", "Hello" ] )
    
    # 2. Operation T1: Transformation 'map'
    pairWordsRDD = inputRDD.map( lambda x : (x, 1) )
    
    # 3. Operation T3: Transformation 'reduceByKey'
    countRDD = pairWordsRDD.reduceByKey( lambda x, y : x + y )

    # 4. Operation T4: Transformation 'map', to screw up the partitioner
    swapTupleRDD = countRDD.map( lambda x : (x[1], x[0]) ) 
    
    # 5. Operation T5: Transformation 'filter', to reduce the amount of words
    filteredRDD = swapTupleRDD.filter( lambda x : x[0] > 1 ) 
    
    # 6. Operation T6: Transformation 'sortByKey', so as to order the entries by decreasing order in the number of appearances.
    solutionRDD = filteredRDD.sortByKey()
    
    # 7. Operation P1: Persist
    solutionRDD.persist()
    
    # 8. Operation A1: Action 'collect'
    resVAL1 = solutionRDD.collect()
    for item in resVAL1:
      print(item) 
    
    # 9. Operation A2: Action 'count'
    resVAL2 = solutionRDD.count()
    print(resVAL2)
        
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
    pass

    # 2. Local or Databricks
    pass

    # 3. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 4. We call to our main function
    my_main(sc)
