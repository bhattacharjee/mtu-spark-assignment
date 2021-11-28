
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
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val input1RDD: RDD[(String, Int)] = sc.parallelize(Array(("A", 1), ("A", 2), ("B", 1), ("C", 1)))

        // 2. Operation C2: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val input2RDD: RDD[(String, Double)] = sc.parallelize(Array(("A", 1.0), ("D", 1.0)))

        // 3. Operation T1: We apply subtractByKey, so as to remove from input1RDD any key appearing in input2RDD
        val subtractedRDD: RDD[(String, Int)] = input1RDD.subtractByKey(input2RDD)

        // 4. Operation A1: 'collect'.
        val resVAL: Array[(String, Int)] = subtractedRDD.collect()

        // 5. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
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
