
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
    // FUNCTION filterLambda
    // ------------------------------------------
    def filterLambda(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
        val inputRDD: RDD[Int] = sc.parallelize(Array(1, 4, 9, 16))

        //2. Operation T1: Transformation 'filter', so as to get a new RDD ('mult4RDD') from inputRDD.
        val mult4RDD: RDD[Int] = inputRDD.filter( (x: Int) => x % 4 == 0)

        //3. Operation A1: 'collect'.
        val resVAL: Array[Int] = mult4RDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // INLINE FUNCTION myFilterFunction
    // ------------------------------------------
    val myFilterFunction: (Int) => Boolean = (x: Int) => {
        //1. We create the output variable
        var res: Boolean = false

        // 2. We apply the filtering function
        if (x % 4 == 0) {
            res = true
        }

        //3. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION filterExplicitFunction
    // ------------------------------------------
    def filterExplicitFunction(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
        val inputRDD: RDD[Int] = sc.parallelize(Array(1, 4, 9, 16))

        //2. Operation T1: Transformation 'filter', so as to get a new RDD ('mult4RDD') from inputRDD.
        val mult4RDD: RDD[Int] = inputRDD.filter(myFilterFunction)

        //3. Operation A1: 'collect'.
        val resVAL: Array[Int] = mult4RDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // INLINE FUNCTION isLength
    // ------------------------------------------
    val isLength: (String, Int) => Boolean = (a: String, b: Int) => {
        //1. We create the output variable
        var res: Boolean = false

        //2. We apply the filtering function
        if (a.length() == b) {
            res = true
        }

        //3. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION filterExplicitFunctionHasMoreThanOneParameter
    // ------------------------------------------
    def filterExplicitFunctionHasMoreThanOneParameter(sc: SparkContext, n: Int): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection ["Hello", "Sun", "Bye", "Cloud"] into an RDD.
        val inputRDD: RDD[String] = sc.parallelize(Array("Hello", "Sun", "Bye", "Cloud"))

        //2. Operation T1: Transformation 'filter', so as to get a new RDD ('size_nRDD') from inputRDD.

        //2.1. We define an alias for the function isLength we want to bypass to
        val isLengthAlias = isLength

        //2.2. We use a lambda to bypass to it
        val sizeN_RDD: RDD[String] = inputRDD.filter( (x: String) => isLengthAlias(x, n) )

        //3. Operation A1: 'collect'.
        val resVAL: Array[String] = sizeN_RDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for (item <- resVAL){
            println(item)
        }
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext, n: Int): Unit = {
        println("\n\n--- [BLOCK 1] filter with F defined via a lambda expression ---")
        filterLambda(sc)

        println("\n\n--- [BLOCK 2] filter with F defined via a explicit function ---")
        filterExplicitFunction(sc)

        println("\n\n--- [BLOCK 3] filter where F requires more than one parameter ---")
        filterExplicitFunctionHasMoreThanOneParameter(sc, n)
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val n: Int = 3

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


