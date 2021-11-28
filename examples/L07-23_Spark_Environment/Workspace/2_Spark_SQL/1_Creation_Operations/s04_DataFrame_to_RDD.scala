
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    // ---------------------------------------------------
    // FUNCTION DF_2_RDD_Row
    // ---------------------------------------------------
    def DF_2_RDD_Row(spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. We define the Schema of our DF.
        val mySchema: StructType = StructType(List(StructField("city", StringType, true), StructField("age", IntegerType, true)))

        // 2. Operation C1: Creation 'read'
        val inputDF: DataFrame = spark.read.format("csv").option("delimiter", ";").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetFile)

        // 3. Operation C2: We revert back to an RDD of Row
        val inputRDD: RDD[Row] = inputDF.rdd

        //4. Operation A1: 'collect'.
        val resVAL: Array[Row] = inputRDD.collect()

        //5. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ---------------------------------------------------------
    // FUNCTION DF_2_Dataset_String
    // ---------------------------------------------------------
    def DF_2_Dataset_String(spark: SparkSession, myDatasetFile: String): Unit = {
        // 1. We define the Schema of our DF.
        val mySchema: StructType = StructType(List(StructField("city", StringType, true), StructField("age", IntegerType, true)))

        // 2. Operation C1: Creation 'read'
        val inputDF: DataFrame = spark.read.format("csv").option("delimiter", ";").option("quote", "").option("header", "false").schema(mySchema).load(myDatasetFile)

        // 3. Operation C2: We convert our DataFrame to a Dataset of String
        val inputDS: Dataset[String] = inputDF.toJSON

        //4. Operation A1: 'collect'.
        val resVAL: Array[String] = inputDS.collect()

        //5. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(spark: SparkSession, option: Int, myDatasetFile: String): Unit = {
        option match {
            case 1  => println("\n\n--- DataFrame to RDD of Row ---")
                DF_2_RDD_Row(spark, myDatasetFile)

            case 2  => println("\n\n--- DataFrame to Dataset of String ---")
                DF_2_Dataset_String(spark, myDatasetFile)
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val option: Int = 2

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        // 3. We setup the log level
        val logger = org.apache.log4j.Logger.getLogger("org")
        logger.setLevel(Level.WARN)

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "/home/nacho/CIT/Tools/MyCode/Spark/"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/2_Spark_SQL/my_dataset/my_tiny_example.csv"

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
