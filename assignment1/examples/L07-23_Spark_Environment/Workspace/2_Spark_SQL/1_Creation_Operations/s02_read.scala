
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


//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    // ------------------------------------------------
    // FUNCTION createDataFrameJSONFileImplicitSchema
    // ------------------------------------------------
    def createDataFrameJSONFileImplicitSchema(spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. Operation C1: Creation 'read'.
        val inputDF: DataFrame = spark.read.format("json").load(myDatasetFile)

        // 2. Operation P1: Persistance of the DF
        inputDF.persist()

        // 3. Operation A1: Print the schema
        inputDF.printSchema()

        // 4. Operation A2: Action of displaying the content of the DF
        inputDF.show()

        // 4. Operation T1: Transformation withColumn
        val newDF: DataFrame = inputDF.withColumn("age", col("age") + 1)

        // 5. Operation A3: Action show
        newDF.show()
    }

    // ----------------------------------------------
    // FUNCTION createDataFrameCSVFileExplicitShema
    // ----------------------------------------------
    def createDataFrameCSVFileExplicitShema(spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. We define the Schema of our DF.
        val mySchema: StructType = StructType(List(StructField("city", StringType, true), StructField("age", IntegerType, true)))

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

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(spark: SparkSession, option: Int, myDatasetFile: String): Unit = {
        option match {
            case 1  => println("\n\n--- DataFrame from JSON File and Implicit Schema ---\n\n")
                createDataFrameJSONFileImplicitSchema(spark, myDatasetFile)

            case 2  => println("\n\n--- DataFrame from CSV File and Explicit Schema ---\n\n")
                createDataFrameCSVFileExplicitShema(spark, myDatasetFile)
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val option: Int = 1

        var fileName : String = ""
        if (option == 1) {
            fileName = "my_example.json"
        }
        else{
            fileName = "my_tiny_example.csv"
        }

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        // 3. We setup the log level
        val logger = org.apache.log4j.Logger.getLogger("org")
        logger.setLevel(Level.WARN)

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/2_Spark_SQL/my_dataset/" + fileName

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
        myMain(spark, option, myDatasetDir)
        println("\n\n\n");
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
