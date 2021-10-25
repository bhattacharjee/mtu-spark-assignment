
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
  // FUNCTION mapLambda
  // ------------------------------------------
  def mapLambda(sc: SparkContext): Unit = {
    //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
    val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

    //2. Operation T1: Transformation 'map', so as to get a new RDD ('squareRDD') from inputRDD.
    val squareRDD: RDD[Int] = inputRDD.map( (x: Int) => x * x)

    //3. Operation A1: 'collect'.
    val resVAL: Array[Int] = squareRDD.collect()

    //4. We print by the screen the collection computed in resVAL
    for (item <- resVAL){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION mySquareFunction
  // ------------------------------------------
  val mySquareFunction: (Int) => Int = (x: Int) => {
    //1. We create the output variable
    val res: Int = x * x

    //2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION mapExplicitFunction
  // ------------------------------------------
  def mapExplicitFunction(sc: SparkContext): Unit = {
    //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
    val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

    //2. Operation T1: Transformation 'map', so as to get a new RDD ('squareRDD') from inputRDD.
    val squareRDD: RDD[Int] = inputRDD.map(mySquareFunction)

    //3. Operation A1: 'collect'.
    val resVAL: Array[Int] = squareRDD.collect()

    //4. We print by the screen the collection computed in resVAL
    for (item <- resVAL){
      println(item)
    }
  }

  // ------------------------------------------
  // INLINE FUNCTION myPower
  // ------------------------------------------
  val myPower: (Int, Int) => Int = (a: Int, b: Int) => {
    //1. We create the output variable
    val res: Int = math.pow(a, b).toInt

    //2. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION mapExplicitFunctionHasMoreThanOneParameter
  // ------------------------------------------
  def mapExplicitFunctionHasMoreThanOneParameter(sc: SparkContext, n: Int): Unit = {
    //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4] into an RDD.
    val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

    //2. Operation T1: Transformation 'map', so as to get a new RDD ('squareRDD') from inputRDD.

    //2.1. We define an alias for the function myPower we want to bypass to
    val myPowerAlias = myPower

    //2.2. We use a lambda to bypass to it
    val squareRDD: RDD[Int] = inputRDD.map( (x: Int) => myPowerAlias(x, n) )

    //3. Operation A1: 'collect'.
    val resVAL: Array[Int] = squareRDD.collect()

    //4. We print by the screen the collection computed in resVAL
    for (item <- resVAL){
      println(item)
    }
  }

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, n: Int): Unit = {
    println("\n\n--- [BLOCK 1] map with F defined via a lambda expression ---")
    mapLambda(sc)

    println("\n\n--- [BLOCK 2] map with F defined via a explicit function ---")
    mapExplicitFunction(sc)

    println("\n\n--- [BLOCK 3] map where F requires more than one parameter ---")
    mapExplicitFunctionHasMoreThanOneParameter(sc, n)
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

