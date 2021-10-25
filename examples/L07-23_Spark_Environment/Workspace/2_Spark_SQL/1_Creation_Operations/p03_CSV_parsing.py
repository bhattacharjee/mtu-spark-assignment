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

import pyspark.sql
import pyspark.sql.functions

# --------------------------------------------------------
# FUNCTION error_createDataFrame_CSV_file_explicit_shema
# --------------------------------------------------------
def error_createDataFrame_CSV_file_explicit_shema(spark, my_dataset_file):
    # 1. We define the Schema of our DF.

    # 1.1. We define the datatype of the first field
    nameRowField = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("surname", pyspark.sql.types.StringType(), True)
         ]
        )

    # 1.2. We define the datatype of the fifth field
    sportRowField = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("sport", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("score", pyspark.sql.types.LongType(), True)
         ]
        )

    sportRowsList = pyspark.sql.types.ArrayType(sportRowField, True)

    # 1.3. We put together all the schema
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("identifier", nameRowField, True),
                                              pyspark.sql.types.StructField("eyes", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("city", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("age", pyspark.sql.types.LongType(), True),
                                              pyspark.sql.types.StructField("likes", sportRowsList, True)
                                             ]
                                            )

    # 2. Operation C1: Creation 'read'
    # ERROR: AnalysisException: 'CSV data source does not support struct<name:string,surname:string> data type.;'
    inputDF = spark.read.format("csv") \
                        .option("delimiter", ";") \
                        .option("quote", "") \
                        .option("header", "false") \
                        .schema(my_schema) \
                        .load(my_dataset_file)

    # 3. Operation A1: Print the schema of the DF my_rawDF
    inputDF.printSchema()

    # 4. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
    inputDF.show()

    # 5. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
    newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 6. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
    newDF.show()

# ------------------------------------------
# FUNCTION my_parser_String2Row
# ------------------------------------------
def my_parser_String2Row(my_string):
    # 1. We create the variable to return
    res = pyspark.sql.Row()

    # 2. We split my_string into its main fields
    fields = my_string.split(";")

    # 3. We edit the first field
    temp = fields[0]

    # 3.1. We get rid of the first and last character
    temp = temp[1: len(temp) - 1]

    # 3.2. We split by commas
    temp = temp.split(",")

    # 3.3. We assign the first field to the result
    nameRow = pyspark.sql.Row(name=temp[0], surname=temp[1])

    # 4. We edit the fifth field
    temp = fields[4]

    # 4.1. We remove the [] symbols
    temp = temp[1: len(temp) - 1]

    # 4.2. We split to get all the tuples in the list
    temp = temp.split("),")

    # 4.3. We create a list of sport Rows
    sportRowsList = []

    # 4.4. We traverse the tuples of the list
    for item in temp:
        # 4.4.1. If it is the last element, we get rid just of both parenthesis
        if (item[len(item) - 1] == ")"):
            item = item[1: len(item) - 1]
        # 4.4.2. Otherwise we just remove the initial parenthesis
        else:
            item = item[1: len(item)]

        # 4.4.3. We split by the comma to get all elements
        values = item.split(",")

        # 4.4.4. We create the sport Row
        sportRow = pyspark.sql.Row(sport=values[0], score=int(values[1]))

        # 4.4.5. We append the sportRow to sportRowsList
        sportRowsList.append(sportRow)

    # 4. We create the Row object associated to my_string
    res = pyspark.sql.Row(identifier=nameRow,
                          eyes=fields[1],
                          city=fields[2],
                          age=int(fields[3]),
                          likes=sportRowsList
                          )

    # 5. We return res
    return res

# ---------------------------------------------------------
# FUNCTION approach_1_read_via_RDD_plus_parser_String2Row
# ---------------------------------------------------------
def approach_1_read_via_RDD_plus_parser_String2Row(sc, spark, my_dataset_file):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the file as an RDD of String.
    inputRDD = sc.textFile(my_dataset_file)

    # 2. Operation T1: Transformation 'map' to pass from an RDD of String to an RDD of Row
    rowRDD = inputRDD.map(my_parser_String2Row)

    # 3. Operation C2: Creation of the DF from the RDD of Rows
    inputDF = spark.createDataFrame(rowRDD)

    # 4. Operation P1: Persistance of the DF inputDF, as we are going to use it twice.
    inputDF.persist()

    # 5. Operation A1: Print the schema of the DF inputDF
    inputDF.printSchema()

    # 6. Operation A1: Action of displaying the content of the DF inputDF, to see what has been read.
    inputDF.show()

    # 7. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. my_jsonDF.
    my_newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 8. Operation A2: Action of displaying the content of the DF my_newDF, to see the column updated.
    my_newDF.show()

# ------------------------------------------
# FUNCTION my_parser_String2tuple
# ------------------------------------------
def my_parser_String2tuple(my_string):
    # 1. We create the variable to return
    res = ()

    # 2. We split my_string into its main fields
    fields = my_string.split(";")

    # 3. We edit the first field
    temp = fields[0]

    # 3.1. We get rid of the first and last character
    temp = temp[1: len(temp) - 1]

    # 3.2. We split by commas
    temp = temp.split(",")

    # 3.3. We assign the first field to the result
    nameRow = tuple(temp)

    # 4. We edit the fifth field
    temp = fields[4]

    # 4.1. We remove the [] symbols
    temp = temp[1: len(temp) - 2]

    # 4.2. We split to get all the tuples in the list
    temp = temp.split("),")

    # 4.3. We create a list of sport Rows
    sportRowsList = []

    # 4.4. We traverse the tuples of the list
    for item in temp:
        # 4.4.1. If it is the last element, we get rid just of both parenthesis
        if (item[len(item) - 1] == ")"):
            item = item[1: len(item) - 1]
        # 4.4.2. Otherwise we just remove the initial parenthesis
        else:
            item = item[1: len(item)]

        # 4.4.3. We split by the comma to get all elements
        values = item.split(",")

        # 4.4.3. We change the datatype of score to be integer
        values[1] = int(values[1])

        # 4.4.4. We create the sport Row
        sportRow = tuple(values)

        # 4.4.5. We append the sportRow to sportRowsList
        sportRowsList.append(sportRow)

    # 5. We modify the datatype of age to be an integer
    fields[3] = int(fields[3])

    # 4. We create the Row object associated to my_string
    res = (nameRow,
           fields[1],
           fields[2],
           fields[3],
           sportRowsList
           )

    # 5. We return res
    return res

# ----------------------------------------------------------------------
# FUNCTION approach_2_read_via_RDD_plus_parser_String2tuple_and_schema
# ----------------------------------------------------------------------
def approach_2_read_via_RDD_plus_parser_String2tuple_and_schema(sc, spark, my_dataset_file):
    # 1. Operation C1: Creation 'textFile', so as to store the content of the file as an RDD of String.
    inputRDD = sc.textFile(my_dataset_file)

    # 2. Operation T1: Transformation 'map' to pass from an RDD of String to an RDD of Row
    rowRDD = inputRDD.map(my_parser_String2tuple)

    # 3. We define the Schema of our DF.

    # 3.1. We define the datatype of the first field
    nameRowField = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("surname", pyspark.sql.types.StringType(), True)
         ]
        )

    # 3.2. We define the datatype of the fifth field
    sportRowField = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("sport", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("score", pyspark.sql.types.LongType(), True)
         ]
        )

    sportRowsList = pyspark.sql.types.ArrayType(sportRowField, True)

    # 3.3. We put together all the schema
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("identifier", nameRowField, True),
                                              pyspark.sql.types.StructField("eyes", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("city", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("age", pyspark.sql.types.LongType(), True),
                                              pyspark.sql.types.StructField("likes", sportRowsList, True)
                                             ]
                                            )

    # 4. Operation C2: Creation of the DF from the RDD of Rows
    inputDF = spark.createDataFrame(rowRDD, my_schema)

    # 5. Operation P1: Persistance of the DF inputDF, as we are going to use it twice.
    inputDF.persist()

    # 6. Operation A1: Print the schema of the DF inputDF
    inputDF.printSchema()

    # 7. Operation A1: Action of displaying the content of the DF inputDF, to see what has been read.
    inputDF.show()

    # 8. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. my_jsonDF.
    my_newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 9. Operation A2: Action of displaying the content of the DF my_newDF, to see the column updated.
    my_newDF.show()

# ------------------------------------------
# FUNCTION my_likes_specific_parser
# ------------------------------------------
def my_likes_specific_parser(my_string):
    # 1. We create the variable to return
    res = []

    # 2. We remove the [] symbols
    temp = my_string[1: len(my_string) - 1]

    # 3. We split to get all the tuples in the list
    temp = temp.split("),")

    # 4. We traverse the tuples of the list
    for item in temp:
        # 4.1. If it is the last element, we get rid just of both parenthesis
        if (item[len(item) - 1] == ")"):
            item = item[1: len(item) - 1]
        # 4.2. Otherwise we just remove the initial parenthesis
        else:
            item = item[1: len(item)]

        # 4.3. We split by the comma to get all elements
        values = item.split(",")

        # 4.4. We change the datatype of score to be integer
        values[1] = int(values[1])

        # 4.5. We create the sport Row
        sportRow = tuple(values)

        # 4.6. We append the sportRow to sportRowsList
        res.append(sportRow)

    # 5. We return res
    return res

# ------------------------------------------------------------------
# FUNCTION approach_3_read_via_DataFrame_and_DSL_operators_and_UDF
# ------------------------------------------------------------------
def approach_3_read_via_DataFrame_and_DSL_operators_and_UDF(spark, my_dataset_file):
    # 1. We define a relaxed Schema of our DF (treating the fields not supported as pure String values).
    my_schema = pyspark.sql.types.StructType([pyspark.sql.types.StructField("identifier", pyspark.sql.types.StringType(), True), #<-- String
                                              pyspark.sql.types.StructField("eyes", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("city", pyspark.sql.types.StringType(), True),
                                              pyspark.sql.types.StructField("age", pyspark.sql.types.IntegerType(), True),
                                              pyspark.sql.types.StructField("likes", pyspark.sql.types.StringType(), True) #<-- String
                                             ]
                                            )

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    text_basedDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_file)

    # 3. Operation P1: We persist the DF
    text_basedDF.persist()

    # 4. Operation A1: Print its schema
    text_basedDF.printSchema()

    # 5. Operation A2: Display its content
    text_basedDF.show()

    # 6. We parse the field "identifier"

    # 6.1. Operation T1: We remove the first and last characters of identifier
    aux1DF = text_basedDF.withColumn("new_identifier",
                                     text_basedDF["identifier"].substr(pyspark.sql.functions.lit(2),
                                                                       pyspark.sql.functions.length(text_basedDF["identifier"]) - 2
                                                                      )
                                    )

    # 6.2. Operation T2: We split by the "," character to get it as a list
    aux2DF = aux1DF.withColumn("name", pyspark.sql.functions.split(aux1DF["new_identifier"], ",")[0])\
                   .withColumn("surname", pyspark.sql.functions.split(aux1DF["new_identifier"], ",")[1])

    # 6.3. Operation T3: We merge the columns as a Struct field
    aux3DF = aux2DF.withColumn("final_identifier", pyspark.sql.functions.struct(aux2DF["name"], aux2DF["surname"]))

    # 6.4. Operation T4: We tidy up the columns
    identifierDF = aux3DF.drop("identifier") \
                         .drop("new_identifier") \
                         .drop("name") \
                         .drop("surname") \
                         .withColumnRenamed("final_identifier", "identifier")

    # 7. We parse the field "sports" using an UDF

    # 7.1. We define the UDF function we will use
    my_likes_specific_parserUDF = pyspark.sql.functions.udf(my_likes_specific_parser,
                                                            pyspark.sql.types.ArrayType(pyspark.sql.types.StructType(
                                                                                        [pyspark.sql.types.StructField("sport", pyspark.sql.types.StringType(), True),
                                                                                         pyspark.sql.types.StructField("score", pyspark.sql.types.LongType(), True)
                                                                                        ]))
                                                           )

    # 7.2. Operation T5: We apply the UDF
    aux4DF = identifierDF.withColumn("new_likes", my_likes_specific_parserUDF(identifierDF["likes"]))

    # 7.3. Operation T6: We tidy up the columns
    inputDF = aux4DF.drop("likes") \
                    .withColumnRenamed("new_likes", "likes")

    # 8. Operation P2: Persistance of the DF inputDF, as we are going to use it twice.
    inputDF.persist()

    # 9. Operation A3: Print the schema of the DF inputDF
    inputDF.printSchema()

    # 10. Operation A4: Action of displaying the content of the DF inputDF, to see what has been read.
    inputDF.show()

    # 11. Operation T6: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. my_jsonDF.
    my_newDF = inputDF.withColumn("age", inputDF["age"] + 1)

    # 12. Operation A5: Action of displaying the content of the DF my_newDF, to see the column updated.
    my_newDF.show()

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(option, sc, spark, my_dataset_file):
    if (option == 1):
        print("\n\n--- ERROR: DataFrame from CSV File and Invalid Explicit Schema ---\n\n")
        error_createDataFrame_CSV_file_explicit_shema(spark, my_dataset_file)

    if (option == 2):
        print("\n\n--- Read the CSV file via: RDD + Parser String -> Row ---\n\n")
        approach_1_read_via_RDD_plus_parser_String2Row(sc, spark, my_dataset_file)

    if (option == 3):
        print("\n\n--- Read the CSV file via: RDD + Parser String -> Regular Tuple and Schema ---\n\n")
        approach_2_read_via_RDD_plus_parser_String2tuple_and_schema(sc, spark, my_dataset_file)

    if (option == 4):
        print("\n\n--- Read the CSV file: DataFrame with Schema, DSL operators to parse Identifier and UDF to parse Likes ---")
        approach_3_read_via_DataFrame_and_DSL_operators_and_UDF(spark, my_dataset_file)

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
    option = 3

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/2_Spark_SQL/my_dataset/my_example.csv"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 6. We call to our main function
    my_main(option, sc, spark, my_dataset_dir)

