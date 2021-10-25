
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
    // FUNCTION combineByKeyLambda
    // ------------------------------------------
    def combineByKeyLambda(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val inputRDD: RDD[(Int, Int)] = sc.parallelize(Array((1, 2), (3, 8), (2, 3), (1, 7), (3, 5), (3, 6), (2, 10)))

        //2. Operation T1: Transformation 'combineByKey', so as to get a new RDD ('combinedRDD') from inputRDD.
        val combinedRDD: RDD[(Int, (Int, Int))] = inputRDD.combineByKey(
            (x: Int) => (x, 1),
            (accum: (Int, Int), newX: Int) => (accum._1 + newX, accum._2 + 1),
            (finalAccum1: (Int, Int), finalAccum2: (Int, Int)) => (finalAccum1._1 + finalAccum2._1, finalAccum1._2 + finalAccum2._2)
        )

        //2. Operation A1: 'collect'.
        val resVAL: Array[(Int, (Int, Int))] = combinedRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for ( item <- resVAL ){
            println(item)
        }
    }

    // ------------------------------------------
    // INLINE FUNCTION createCombiner
    // ------------------------------------------
    val createCombiner: (Int) => (Int, Int) = (value: Int) => {
        //1. We create the output variable
        var res: (Int, Int) = (value, 1)

        //2. We return res
        res
    }

    // ------------------------------------------
    // INLINE FUNCTION mergeValue
    // ------------------------------------------
    val mergeValue: ((Int, Int), Int) => (Int, Int) = (accum: (Int, Int), newValue: Int) => {
        //1. We create the output variable with the content of currentAccumulator
        var res: (Int, Int) = (0,0)

        //2. We modify the value of res
        val val1: Int = accum._1 + newValue
        val val2: Int = accum._2 + 1

        //3. We assign res properly
        res = (val1, val2)

        //4. We return res
        res
    }

    // ------------------------------------------
    // INLINE FUNCTION mergeCombiners
    // ------------------------------------------
    val mergeCombiners: ((Int, Int), (Int, Int)) => (Int, Int) = (accum1: (Int, Int), accum2: (Int, Int)) => {
        //1. We create the output variable with the content of accumulator1
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
    // FUNCTION combineByKeyExplicitFunction
    // ------------------------------------------
    def combineByKeyExplicitFunction(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val inputRDD: RDD[(Int, Int)] = sc.parallelize(Array((1, 2), (3, 8), (2, 3), (1, 7), (3, 5), (3, 6), (2, 10)))

        //2. Operation T1: Transformation 'combineByKey', so as to get a new RDD ('combinedRDD') from inputRDD.
        val combinedRDD: RDD[(Int, (Int, Int))] = inputRDD.combineByKey( createCombiner, mergeValue, mergeCombiners )

        //2. Operation A1: 'collect'.
        val resVAL: Array[(Int, (Int, Int))] = combinedRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for ( item <- resVAL ){
            println(item)
        }
    }

    // ------------------------------------------
    // INLINE FUNCTION function1
    // ------------------------------------------
    val function1: (Int) => Map[Int, Int] = (firstValue: Int) => {
        //1. We create the output variable
        var res: Map[Int, Int] = Map[Int, Int]()

        //2. We modify the value of res
        res = res + (firstValue -> 1)

        //3. We return res
        res
    }

    // ------------------------------------------
    // INLINE FUNCTION function2
    // ------------------------------------------
    val function2: (Map[Int, Int], Int) => Map[Int, Int] = (currentAccumulator: Map[Int, Int], newValue: Int) => {
        //1. We create the output variable with the content of currentAccumulator
        var res: Map[Int, Int] = Map[Int, Int]()

        //2. We traverse currentAccumulator to update res with its keys
        for ( (k,v) <- currentAccumulator ){
            res = res + (k -> v)
        }

        //3. We check whether the new value is in res, and update it accordingly
        if (res.contains(newValue)){
            val cv: Int = res(newValue)
            res = res + (newValue -> (cv + 1))
        }
        else{
            res = res + (newValue -> 1)
        }

        //4. We return res
        res
    }

    // ------------------------------------------
    // INLINE FUNCTION function3
    // ------------------------------------------
    val function3: (Map[Int, Int], Map[Int, Int]) => Map[Int, Int] = (accumulator1: Map[Int, Int], accumulator2: Map[Int, Int]) => {
        //1. We create the output variable with the content of accumulator1
        var res: Map[Int, Int] = Map[Int, Int]()

        //2. We traverse currentAccumulator to update res with its keys
        for ( (k1,v1) <- accumulator1 ){
            res = res + (k1 -> v1)
        }

        //3. We traverse accumulator2 to update res with its keys
        for ( (k2,v2) <- accumulator2 ){
            if (res.contains(k2)){
                val v1: Int = res(k2)
                res = res + (k2 -> (v1 + v2))
            }
            else{
                res = res + (k2 -> v2)
            }
        }

        //4. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION combineByKeyComplex
    // ------------------------------------------
    def combineByKeyComplex(sc: SparkContext): Unit = {
        //1. Operation C1: Creation 'parallelize', so as to store the content of the collection into an RDD.
        val inputRDD: RDD[(String, Int)] = sc.parallelize(Array(("Hello", 1), ("Hello", 2), ("Goodbye", 2), ("Hello", 3), ("Hello", 1), ("Hello", 1), ("Goodbye", 2), ("Hello", 3)))

        //2. Operation T1: Transformation 'combineByKey', so as to get a new RDD ('combinedRDD') from inputRDD.
        val combinedRDD: RDD[(String, Map[Int, Int])] = inputRDD.combineByKey( function1, function2, function3 )

        //2. Operation A1: 'collect'.
        val resVAL: Array[(String, Map[Int, Int])] = combinedRDD.collect()

        //4. We print by the screen the collection computed in resVAL
        for ( item <- resVAL ){
            for ( (k, v) <- item._2 ){
                println(item._1 + " -> " + k + " : " + v)
            }
        }
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext): Unit = {
        println("\n\n--- [BLOCK 1] combineByKey with F1, F2, F3 defined via a lambda expressions ---")
        combineByKeyLambda(sc)

        println("\n\n--- [BLOCK 2] combineByKey with F1, F2, F3 defined via a explicit functions ---")
        combineByKeyExplicitFunction(sc)

        println("\n\n--- [BLOCK 3] combineByKey with a more complicated functionality ---")
        combineByKeyComplex(sc)
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
