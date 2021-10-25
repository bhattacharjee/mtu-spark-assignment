
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
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val input1RDD: RDD[(String, Int)] = sc.parallelize(Array(("A", 1), ("A", 2), ("B", 1), ("C", 1)))

        // 2. Operation C2: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val input2RDD: RDD[(String, Double)] = sc.parallelize(Array(("A", 1.0), ("B", 1.0), ("B", 2.0), ("D", 1.0)))

        // 3. Operation T1: We apply a set transformation based on the option being passed
        var joinRDD: RDD[(String, (Int, Double))] = null
        var leftOuterJoinRDD: RDD[(String, (Int, Option[Double]))] = null
        var rightOuterJoinRDD: RDD[(String, (Option[Int], Double))] = null
        var cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Double]))] = null

        option match {
            case 1 => joinRDD = input1RDD.join(input2RDD)
            case 2 => leftOuterJoinRDD = input1RDD.leftOuterJoin(input2RDD)
            case 3 => rightOuterJoinRDD = input1RDD.rightOuterJoin(input2RDD)
            case 4 => cogroupRDD = input1RDD.cogroup(input2RDD)
        }

        // 4. Operation A1: 'collect'.
        var res1VAL: Array[(String, (Int, Double))] = Array[(String, (Int, Double))]()
        var res2VAL: Array[(String, (Int, Option[Double]))] = Array[(String, (Int, Option[Double]))]()
        var res3VAL: Array[(String, (Option[Int], Double))] = Array[(String, (Option[Int], Double))]()
        var res4VAL: Array[(String, (Iterable[Int], Iterable[Double]))] = Array[(String, (Iterable[Int], Iterable[Double]))]()

        option match {
            case 1 => res1VAL = joinRDD.collect()
            case 2 => res2VAL = leftOuterJoinRDD.collect()
            case 3 => res3VAL = rightOuterJoinRDD.collect()
            case 4 => res4VAL = cogroupRDD.collect()
        }

        // 5. We print by the screen the collection computed in resVAL
        for (item <- res1VAL){
            println(item)
        }
        for (item <- res2VAL){
            println(item)
        }
        for (item <- res3VAL){
            println(item)
        }
        for (item <- res4VAL){
            println(item)
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val option: Int = 4

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
