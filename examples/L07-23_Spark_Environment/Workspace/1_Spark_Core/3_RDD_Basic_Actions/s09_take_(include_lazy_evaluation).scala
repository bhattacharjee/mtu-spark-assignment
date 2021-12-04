
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext, myDatasetDir: String, longitude: Int, numLines: Int): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD: RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation T1: Transformation 'filter', so as to get a new RDD ('filteredRDD') with lines having at list length 'longitude'.
        val filteredRDD: RDD[String] = inputRDD.filter( (x: String) => x.length() >= longitude )

        // 3. Operation A1: Action 'take', so as to take only a subset of the items of the RDD.
        val resVAL: Array[String] = filteredRDD.take(numLines)

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val longitude: Int = 20
        val numLines: Int = 2

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        // 3. We setup the log level
        val logger = org.apache.log4j.Logger.getLogger("org")
        logger.setLevel(Level.WARN)

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/1_Spark_Core/my_dataset/"

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

        // 6. We call to myMain
        myMain(sc, myDatasetDir, longitude, numLines)
        println("\n\n\n");
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

