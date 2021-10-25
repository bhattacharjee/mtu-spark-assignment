
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
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection ["This is my first line", "Another line here"] into an RDD.
        // As we see, in this case our RDD is a collection of String items.
        val inputRDD: RDD[String] = sc.parallelize(Array("This is my first line", "Another line here"))

        // 2. Operation T1: Transformation 'map', so as to get a new RDD ('wordsRDD') from inputRDD.
        val wordsRDD: RDD[String] = inputRDD.flatMap( (line) => line.split(" "))

        // 3. Operation P1: We persist wordsRDD, as we are going to use it more than once.
        wordsRDD.persist()

        // 4. Operation A1: We count how many items are in the collection wordsRDD, to ensure there are 8 and not 2.
        val res1VAL: Long = wordsRDD.count()

        // 5. We print by the screen the result value res1VAL
        println(res1VAL)

        // 6. Operation A2: collect the items from wordsRDD
        val res2VAL: Array[String] = wordsRDD.collect()

        // 7. We print by the screen the collection computed in res2VAL
        for (item <- res2VAL) {
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


