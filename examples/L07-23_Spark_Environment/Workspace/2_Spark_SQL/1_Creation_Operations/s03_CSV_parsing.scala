
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer


//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    // ---------------------------------------------------
    // FUNCTION errorCreateDataFrameCSVFileExplicitShema
    // ---------------------------------------------------
    def errorCreateDataFrameCSVFileExplicitShema(spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. We define the Schema of our DF.

        // 1.1. We define the datatype of the first field
        val nameRowField = StructType(List(StructField("name", StringType, true), StructField("surname", StringType, true)))

        // 1.2. We define the datatype of the fifth field
        val sportRowField = StructType(List(StructField("sport", StringType, true), StructField("score", IntegerType, true)))

        val sportRowsList = ArrayType(sportRowField, true)

        // 1.3. We put together all the schema
        val mySchema: StructType = StructType(List(StructField("identifier", nameRowField, true), StructField("eyes", StringType, true),
            StructField("city", StringType, true), StructField("age", IntegerType, true),
            StructField("likes", sportRowsList, true)))

        // 2. Operation C1: Creation 'read'
        val inputDF: DataFrame = spark.read.format("csv").option("delimiter", ";").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetFile)

        // 3. Operation A1: Print the schema of the DF my_rawDF
        inputDF.printSchema()

        // 4. Operation A2: Action of displaying the content of the DF my_rawDF, to see what has been read.
        inputDF.show()

        // 5. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. myRowDF.
        val newDF: DataFrame = inputDF.withColumn("age", col("age") + 1)

        // 6. Operation A3: Action of displaying the content of the DF my_newDF, to see the column updated.
        newDF.show()
    }

    // ------------------------------------------
    // INLINE FUNCTION myParserString2Row
    // ------------------------------------------
    val myParserString2Row: (String) => Row = (myString: String) => {
        //1. We create the output variable
        var res: Row = Row()

        // 2. We split my_string into its main fields
        val fields: Array[String] = myString.split(";")

        // 3. We edit the first field
        var temp: String = fields(0)

        // 3.1. We get rid of the first and last character
        temp = temp.slice(1, temp.length() - 1)

        // 3.2. We split by commas
        val myNameSurname: Array[String] = temp.split(",")

        // 3.3. We assign the first field to the result
        val nameRow = Row(myNameSurname(0), myNameSurname(1))

        // 4. We edit the fifth field
        temp = fields(4)

        // 4.1. We remove the [] symbols
        temp = temp.slice(1, temp.length() - 1)

        // 4.2. We split to get all the tuples in the list
        val mySports: Array[String] =  temp.split("\\),")

        // 4.3. We create a list of sport Rows
        val sportRowsBuffer: ListBuffer[Row] = new ListBuffer[Row]()

        // 4.4. We traverse the tuples of the list
        for (item <- mySports){
            var myItem: String = item

            // 4.4.1. If it is the last element, we get rid just of both parenthesis
            if (myItem.apply(myItem.length() - 1) == ')'){
                myItem = myItem.slice(1, myItem.length() - 1)
            }
            // 4.4.2. Otherwise we just remove the initial parenthesis
            else{
                myItem = myItem.slice(1, myItem.length())
            }

            // 4.4.3. We split by the comma to get all elements
            val values: Array[String] = myItem.split(",")

            // 4.4.4. We create the sport Row
            val sportRow: Row = Row(values(0), values(1).toInt)

            // 4.4.5. We append the sportRow to sportRowsList
            sportRowsBuffer += sportRow
        }

        val sportRowsList: List[Row] = sportRowsBuffer.toList

        // 5. We create the Row object associated to my_string
        res = Row(nameRow, fields(1), fields(2), fields(3).toInt, sportRowsList)

        // 6. We return res
        res
    }

    // ---------------------------------------------------------
    // FUNCTION approach_1_ReadViaRDDPlusParserString2Row
    // ---------------------------------------------------------
    def approach_1_ReadViaRDDPlusParserString2Row(sc: SparkContext, spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the file as an RDD of String.
        val inputRDD: RDD[String] = sc.textFile(myDatasetFile)

        // 2. Operation T1: Transformation 'map' to pass from an RDD of String to an RDD of Row
        val rowRDD: RDD[Row] = inputRDD.map(myParserString2Row)

        // 3. We define the Schema of our DF.

        // 3.1. We define the datatype of the first field
        val nameRowField = StructType(List(StructField("name", StringType, true), StructField("surname", StringType, true)))

        // 3.2. We define the datatype of the fifth field
        val sportRowField = StructType(List(StructField("sport", StringType, true), StructField("score", IntegerType, true)))

        val sportRowsList = ArrayType(sportRowField, true)

        // 3.3. We put together all the schema
        val mySchema: StructType = StructType(List(StructField("identifier", nameRowField, true), StructField("eyes", StringType, true),
            StructField("city", StringType, true), StructField("age", IntegerType, true),
            StructField("likes", sportRowsList, true)))

        // 4. Operation C2: Creation of the DF from the RDD of Rows
        val inputDF: DataFrame = spark.createDataFrame(rowRDD, mySchema)

        // 5. Operation P1: Persistance of the DF inputDF, as we are going to use it twice.
        inputDF.persist()

        // 6. Operation A1: Print the schema of the DF inputDF
        inputDF.printSchema()

        // 7. Operation A1: Action of displaying the content of the DF inputDF, to see what has been read.
        inputDF.show()

        // 8. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. my_jsonDF.
        val myNewDF: DataFrame = inputDF.withColumn("age", col("age") + 1)

        // 9. Operation A2: Action of displaying the content of the DF my_newDF, to see the column updated.
        myNewDF.show()
    }

    // ------------------------------------------
    // INLINE FUNCTION myParserString2Row
    // ------------------------------------------
    val myLikesSpecificParser: (String) => List[Tuple2[String, Int]] = (myString: String) => {
        // 1. We create the output variable
        var res: List[Tuple2[String, Int]] = List()

        // 2. We create a list of sport Rows
        val sportRowsBuffer: ListBuffer[Tuple2[String, Int]] = new ListBuffer[Tuple2[String, Int]]()

        // 3. We remove the [] symbols
        val myNewString = myString.slice(1, myString.length() - 1)

        // 4. We split to get all the tuples in the list
        val mySports: Array[String] =  myNewString.split("\\),")

        // 5. We traverse the tuples of the list
        for (item <- mySports){
            var myItem: String = item

            // 5.1. If it is the last element, we get rid just of both parenthesis
            if (myItem.apply(myItem.length() - 1) == ')'){
                myItem = myItem.slice(1, myItem.length() - 1)
            }
            // 5.2. Otherwise we just remove the initial parenthesis
            else{
                myItem = myItem.slice(1, myItem.length())
            }

            // 5.3. We split by the comma to get all elements
            val values: Array[String] = myItem.split(",")

            // 5.4. We create the sport Row
            val sportRow: Tuple2[String, Int] = (values(0), values(1).toInt)

            // 5.5. We append the sportRow to sportRowsList
            sportRowsBuffer += sportRow
        }

        // 6. We assign res
        res = sportRowsBuffer.toList

        // 7. We return res
        res
    }

    // -----------------------------------------------------------
    // FUNCTION approach_2_ReadViaDataFrameAndDSLOperatorsAndUDF
    // -----------------------------------------------------------
    def approach_2_ReadViaDataFrameAndDSLOperatorsAndUDF(sc: SparkContext, spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. We define the schema of our DF
        val mySchema: StructType = StructType(List(StructField("identifier", StringType, true), StructField("eyes", StringType, true),
            StructField("city", StringType, true), StructField("age", IntegerType, true),
            StructField("likes", StringType, true)))

        // 2. Operation C1: Creation 'read'
        val text_basedDF: DataFrame = spark.read.format("csv").option("delimiter", ";").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetFile)

        // 3. Operation P1: We persist the DF
        text_basedDF.persist()

        // 4. Operation A1: Print its schema
        text_basedDF.printSchema()

        // 5. Operation A2: Display its content
        text_basedDF.show()

        // 6.1. Operation T1: We remove the first and last characters of identifier
        val aux1DF: DataFrame = text_basedDF.withColumn("new_identifier", expr("substring(identifier, 2, length(identifier)-2)"))

        // 6.2. Operation T2: We split by the "," character to get it as a list
        val aux2DF: DataFrame = aux1DF.withColumn("name", split(col("new_identifier"), ",")(0))
          .withColumn("surname", split(col("new_identifier"), ",")(1))

        // 6.3. Operation T3: We merge the columns as a Struct field
        val aux3DF: DataFrame = aux2DF.withColumn("final_identifier", struct(col("name"), col("surname")))

        // 6.4. Operation T4: We tidy up the columns
        val identifierDF: DataFrame = aux3DF.drop("identifier").drop("new_identifier").drop("name").drop("surname").withColumnRenamed("final_identifier", "identifier")

        // 7. We parse the field "sports" using an UDF

        // 7.1. We define the UDF function we will use
        val my_likes_specific_parserUDF = udf(myLikesSpecificParser)

        // 7.2. Operation T5: We apply the UDF
        val aux4DF: DataFrame = identifierDF.withColumn("new_likes", my_likes_specific_parserUDF(col("likes")))

        // 7.3. Operation T6: We tidy up the columns
        val inputDF: DataFrame = aux4DF.drop("likes").withColumnRenamed("new_likes", "likes")

        // 8. Operation P1: Persistance of the DF inputDF, as we are going to use it twice.
        inputDF.persist()

        // 9. Operation A1: Print the schema of the DF inputDF
        inputDF.printSchema()

        // 10. Operation A1: Action of displaying the content of the DF inputDF, to see what has been read.
        inputDF.show()

        // 11. Operation T1: Transformation withColumn, which returns a new DF my_newDF where the age is incremented w.r.t. my_jsonDF.
        val myNewDF: DataFrame = inputDF.withColumn("age", col("age") + 1)

        // 12. Operation A2: Action of displaying the content of the DF my_newDF, to see the column updated.
        myNewDF.show()
    }

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext, spark: SparkSession, option: Int, myDatasetFile: String): Unit = {
        option match {
            case 1  => println("\n\n--- ERROR: DataFrame from CSV File and Invalid Explicit Schema ---\n\n")
                errorCreateDataFrameCSVFileExplicitShema(spark, myDatasetFile)

            case 2  => println("\n\n--- Read the CSV file via: RDD + Parser String -> Row and Schema ---\n\n")
                approach_1_ReadViaRDDPlusParserString2Row(sc, spark, myDatasetFile)

            case 3  => println("\n\n--- Read the CSV file: DataFrame with Schema, DSL operators to parse Identifier and UDF to parse Likes ---")
                approach_2_ReadViaDataFrameAndDSLOperatorsAndUDF(sc, spark, myDatasetFile)
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val option: Int = 3

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        // 3. We setup the log level
        val logger = org.apache.log4j.Logger.getLogger("org")
        logger.setLevel(Level.WARN)

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/2_Spark_SQL/my_dataset/my_example.csv"

        if (localFalseDatabricksTrue == false) {
            myDatasetDir = myLocalPath + myDatasetDir
        }
        else {
            myDatasetDir = myDatabricksPath + myDatasetDir
        }

        // 5. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 5.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 5.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 5.1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        // 5.2. Databricks
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 6. We configure the Spark variable
        val spark : SparkSession = SparkSession.builder().getOrCreate()
        Logger.getRootLogger.setLevel(Level.WARN)
        println("\n\n\n");

        // 7. We call to myMain
        myMain(sc, spark, option, myDatasetDir)
        println("\n\n\n");
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
