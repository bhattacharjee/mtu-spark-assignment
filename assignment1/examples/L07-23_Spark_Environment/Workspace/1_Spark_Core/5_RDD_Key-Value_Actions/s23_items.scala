
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
    def myMain(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4]
        val inputRDD: RDD[(Int, Int)] = sc.parallelize(Array((1, 2), (3, 4), (3, 6)))

        //2. Operation A1: 'collect'.
        val resVAL: Map[Int, Long] = inputRDD.countByKey().toMap

        //3. We print by the screen the collection computed in resVAL
        for ((k, v) <- resVAL){
            println(k + " -> " + v)
        }
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

        // 4. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 4.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 4.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 4.1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        // 4.2. Databricks
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 5. We call to myMain
        myMain(sc)
        println("\n\n\n");
    }

}


//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
