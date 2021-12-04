
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

    // ------------------------------------------
    // INLINE FUNCTION myMult
    // ------------------------------------------
    val myMult: (Int, Int) => Int = (x: Int, y: Int) => {
        //1. We create the output variable
        val res: Int = x * y

        //2. We return res
        res
    }

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4]
        val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

        // 2. Operation P1: We persist inputRDD, as we are going to use it more than once.
        inputRDD.persist()

        // 3. Operation A1: Action 'reduce', so as to get one aggregated value from inputRDD.
        val res1VAL: Int = inputRDD.reduce( (x: Int, y: Int) => x + y)

        // 4. We print by the screen the result computed in res1VAL
        println(res1VAL)

        // 5. Operation A2: Action 'reduce', so as to get one aggregated value from inputRDD.
        val res2VAL: Int = inputRDD.reduce(myMult)

        // 6. We print by the screen the result computed in res1VAL
        println(res2VAL)
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
