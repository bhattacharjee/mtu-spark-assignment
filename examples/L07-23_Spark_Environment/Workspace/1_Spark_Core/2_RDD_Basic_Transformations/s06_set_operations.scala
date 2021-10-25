
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
    def myMain(sc: SparkContext, option: Int): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1, 2, 1, 3] into an RDD.
        val input1RDD: RDD[Int] = sc.parallelize(Array(1, 2, 1, 3))

        // 2. Operation C2: Creation 'parallelize', so as to store the content of the collection [1,3,4] into an RDD.
        val input2RDD: RDD[Int] = sc.parallelize(Array(1, 3, 4))

        // 3. Operation T1: We apply a set transformation based on the option being passed
        var setResultRDD: RDD[Int] = null
        var cartesianResultRDD: RDD[(Int, Int)] = null

        // Please note that all operations except 2 require shuffling all the data over the network to ensure that the operation is performed correctly.
        // Shuffling the data is undesirable (both in terms of time and network congestion), so use it only when really needed.
        // Also note that the operation 2 does not require to shuffle the data just because Spark union allows duplicates
        // (which is a bit different from the semantics of classical union operation in set theory).
        option match {
            case 1 => setResultRDD = input1RDD.distinct()
            case 2 => setResultRDD = input1RDD.union(input2RDD)
            case 3 => setResultRDD = input1RDD.intersection(input2RDD)
            case 4 => setResultRDD = input1RDD.subtract(input2RDD)
            case 5 => cartesianResultRDD = input1RDD.cartesian(input2RDD)
        }

        // 4. Operation A1: We collect the results
        if (setResultRDD != null){
            val res1VAL: Array[Int] = setResultRDD.collect()
            for (item <- res1VAL){
                println(item)
            }
        }

        if (cartesianResultRDD != null){
            val res2VAL: Array[(Int, Int)] = cartesianResultRDD.collect()
            for (item <- res2VAL){
                println(item)
            }
        }

    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val option: Int = 5

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
        myMain(sc, option)
        println("\n\n\n");
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

