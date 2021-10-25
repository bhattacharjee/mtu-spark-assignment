
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    // ------------------------------------------
    // FUNCTION myKeys
    // ------------------------------------------
    def myKeys(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "A"), (1, "B"), (2, "B")] into an RDD.
        val inputRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "A"), (1, "B"), (2, "B")))

        // 2. Operation T1: Transformation 'keys', so as to get a new RDD with the keys in inputRDD.
        val keysRDD: RDD[Int] = inputRDD.keys

        //3. Operation A1: 'collect'.
        val resVAL: Array[Int] = keysRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // FUNCTION myValues
    // ------------------------------------------
    def myValues(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [(1, "A"), (1, "B"), (2, "B")] into an RDD.
        val inputRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "A"), (1, "B"), (2, "B")))

        // 2. Operation T1: Transformation 'values', so as to get a new RDD with the values in inputRDD.
        val valuesRDD: RDD[String] = inputRDD.values

        //3. Operation A1: 'collect'.
        val resVAL: Array[String] = valuesRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext, n: Int): Unit = {
        println("\n\n--- [BLOCK 1] keys ---")
        myKeys(sc)

        println("\n\n--- [BLOCK 2] values ---")
        myValues(sc)
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val n: Int = 5

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        //3. We setup the log level
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

            // 4.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 4. We call to myMain
        myMain(sc, n)
        println("\n\n\n");
    }

}


//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

