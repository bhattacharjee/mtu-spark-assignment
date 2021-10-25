
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
    // FUNCTION myMapValues
    // ------------------------------------------
    def myMapValues(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val inputRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "hello world"), (2, "bye bye"), (1, "hello nice to see you")))

        // 2. Operation T1: Transformation 'mapValues', so as to get a the lenght of each value per (key, value) in inputRDD.
        val mappedRDD: RDD[(Int, Int)] = inputRDD.mapValues( (x: String) => (x.split(" ")).length)

        //3. Operation A1: 'collect'.
        val resVAL: Array[(Int, Int)] = mappedRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // FUNCTION myFlatMapValues
    // ------------------------------------------
    def myFlatMapValues(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val inputRDD: RDD[(Int, String)] = sc.parallelize(Array((1, "hello world"), (2, "bye bye"), (1, "hello nice to see you")))

        // 2. Operation T1: Transformation 'flatMapValues', so as to get a the lenght of each value per (key, value) in inputRDD.
        val mappedRDD: RDD[(Int, String)] = inputRDD.flatMapValues( (x: String) => x.split(" ") )

        //3. Operation A1: 'collect'.
        val resVAL: Array[(Int, String)] = mappedRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext, n: Int): Unit = {
        println("\n\n--- [BLOCK 1] mapValues ---")
        myMapValues(sc)

        println("\n\n--- [BLOCK 2] flatMapValues ---")
        myFlatMapValues(sc)
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

