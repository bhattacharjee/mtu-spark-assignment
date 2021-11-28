

//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.commons.io.FileUtils
import java.io._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.rdd.RDD

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext): Unit = {
        // 1. Operation C1: Creation 'parallelize'
        val inputRDD: RDD[String] = sc.parallelize(Array("Hello", "Hola", "Bonjour", "Hello", "Bonjour", "Ciao", "Hello"))

        // 2. Operation T2: Transformation 'map'
        val pairWordsRDD: RDD[(String, Int)] = inputRDD.map( (x: String) => (x, 1) )

        // 3. Operation T3: Transformation 'reduceByKey'
        val countRDD: RDD[(String, Int)] = pairWordsRDD.reduceByKey( (x: Int, y: Int) => x + y )

        // 4. Operation T4: Transformation 'map', to screw up the partitioner
        val swapTupleRDD: RDD[(Int, String)] = countRDD.map( (x: (String, Int)) => (x._2, x._1) ) 
      
        // 5. Operation T5: Transformation 'filter', to reduce the amount of words
        val filteredRDD: RDD[(Int, String)] = swapTupleRDD.filter( (x: (Int, String)) => x._1 > 1 ) 
      
        // 6. Operation T6: Transformation 'sortByKey', so as to order the entries by decreasing order in the number of appearances.
        val solutionRDD: RDD[(Int, String)] = filteredRDD.sortByKey()

        // 7. Operation P1: Persist
        solutionRDD.persist()
      
        // 8. Operation A1: Action 'collect'
        val resVAL1: Array[(Int, String)] = solutionRDD.collect()
        for (item <- resVAL1){
          println(item)
        }
      
        // 9. Operation A2: Action 'count'
        val resVAL2: Long = solutionRDD.count()
        println(resVAL2)

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
        // 4.2. Databricks Mode
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 5. We call to myMain
        myMain(sc)
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
