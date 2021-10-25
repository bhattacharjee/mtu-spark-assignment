
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.commons.io.FileUtils
import java.io._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.rdd.RDD

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext, myDatasetDir: String, myResultDir: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD: RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation A1: Action 'saveAsTextFile', so as to store the content of inputRDD into the desired DBFS folder.
        inputRDD.saveAsTextFile(myResultDir)
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        // 3. We setup the log level
        val logger = org.apache.log4j.Logger.getLogger("org")
        logger.setLevel(Level.WARN)

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/1_Spark_Core/my_dataset/"
        var myResultDir : String = "FileStore/tables/1_Spark_Core/my_result/"

        if (localFalseDatabricksTrue == false) {
            myDatasetDir = myLocalPath + myDatasetDir
            myResultDir = myLocalPath + myResultDir
        }
        else {
            myDatasetDir = myDatabricksPath + myDatasetDir
            myResultDir = myDatabricksPath + myResultDir
        }

        // 5. We remove my_result directory
        if (localFalseDatabricksTrue == false) {
            FileUtils.deleteDirectory(new File(myResultDir))
        }
        else {
            dbutils.fs.rm(myResultDir, true)
        }

        // 6. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 6.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 6.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 6.1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        // 6.2. Databricks
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 7. We call to myMain
        myMain(sc, myDatasetDir, myResultDir)
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
