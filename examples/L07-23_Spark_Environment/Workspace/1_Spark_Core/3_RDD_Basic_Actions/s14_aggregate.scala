
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
    // FUNCTION averageMapAndReduce
    // ------------------------------------------
    def averageMapAndReduce(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.
        val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

        //2. Operation T1: Transformation 'map', so as to get a new RDD ('pairRDD') from inputRDD.
        val pairRDD: RDD[(Int, Int)] = inputRDD.map( (x: Int) => (x, 1))

        //3. Operation A1: Action 'reduce', so as to get one aggregated value from pairRDD.
        val resVAL: (Int, Int) = pairRDD.reduce( (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2) )

        // 4. We print by the screen the collection computed in resVAL
        println("TotalSum = " + resVAL._1)
        println("TotalItems = " + resVAL._2)
        println("AverageValue = " + (resVAL._1).toFloat / (resVAL._2).toFloat)
    }

    // ------------------------------------------
    // FUNCTION aggregateLambda
    // ------------------------------------------
    def aggregateLambda(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.
        val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

        //2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.
        val resVAL: (Int, Int) = inputRDD
          .aggregate((0, 0))( (acc: (Int, Int), e: Int) => (acc._1 + e, acc._2 + 1) , (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) )

        // 3. We print by the screen the collection computed in resVAL
        println("TotalSum = " + resVAL._1)
        println("TotalItems = " + resVAL._2)
        println("AverageValue = " + (resVAL._1).toFloat / (resVAL._2).toFloat)
    }

    // ------------------------------------------
    // INLINE FUNCTION combineLocalNode
    // ------------------------------------------
    val combineLocalNode: ((Int, Int), Int) => (Int, Int) = (accum: (Int, Int), item: Int) => {
        //1. We create the output variable
        var res: (Int, Int) = (0,0)

        //2. We modify the value of res
        val val1: Int = accum._1 + item
        val val2: Int = accum._2 + 1

        //3. We assign res properly
        res = (val1, val2)

        //4. We return res
        res
    }

    // ------------------------------------------
    // INLINE FUNCTION combineDifferentNodes
    // ------------------------------------------
    val combineDifferentNodes: ((Int, Int), (Int, Int)) => (Int, Int) = (accum1: (Int, Int), accum2: (Int, Int)) => {
        //1. We create the output variable
        var res: (Int, Int) = (0,0)

        //2. We modify the value of res
        val val1: Int = accum1._1 + accum2._1
        val val2: Int = accum1._2 + accum2._2

        //3. We assign res properly
        res = (val1, val2)

        //4. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION aggregateOwnFunctions
    // ------------------------------------------
    def aggregateOwnFunctions(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection [1,2,3,4,5] into an RDD.
        val inputRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5))

        //2. Operation A1: Action 'aggregate', to aggregate all items of the RDD returning a value of a different datatype that such RDD items.
        val resVAL: (Int, Int) = inputRDD.aggregate((0, 0))(combineLocalNode, combineDifferentNodes)

        // 3. We print by the screen the collection computed in resVAL
        println("TotalSum = " + resVAL._1)
        println("TotalItems = " + resVAL._2)
        println("AverageValue = " + (resVAL._1).toFloat / (resVAL._2).toFloat)
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext): Unit = {
        println("\n\n--- [BLOCK 1] Compute Average via map + reduce ---")
        averageMapAndReduce(sc)

        println("\n\n--- [BLOCK 2] Compute Average via aggregate with lambda ---")
        aggregateLambda(sc)

        println("\n\n--- [BLOCK 3] Compute Average via aggregate with our own functions ---")
        aggregateOwnFunctions(sc)
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed

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
        myMain(sc)
        println("\n\n\n");
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())

