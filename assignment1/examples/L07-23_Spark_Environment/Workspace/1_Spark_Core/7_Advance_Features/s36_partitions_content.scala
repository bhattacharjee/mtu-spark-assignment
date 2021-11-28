
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
    // INLINE FUNCTION myWhatToDoWithThePartition
    // ------------------------------------------
    val myWhatToDoWithThePartition: (Int, Iterator[String]) => Iterator[(Int, String)] = (partitionIndex: Int, partitionContentIterator: Iterator[String]) => {
        //1. We create the output variable with the content of currentAccumulator
        var res: Iterator[(Int, String)] = partitionContentIterator.toList.map( partitionItem => (partitionIndex, partitionItem) ).iterator

        //2. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext, myDatasetDir: String, longitude: Int): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD : RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation T1: Transformation 'filter', so as to get a new RDD ('filteredRDD') with lines having at list length 'longitude'.
        val filteredRDD: RDD[String] = inputRDD.filter( (x: String) => x.length() >= longitude )

        // 3. Operation T2: We use mapPartitions to see how many partitions are there
        val contentRDD : RDD[(Int, String)] = filteredRDD.mapPartitionsWithIndex( myWhatToDoWithThePartition, true )

        // 4. Operation A1: collect the items from inputRDDSizes
        val resVAL: Array[(Int, String)] = contentRDD.collect()

        // 5. We print by the screen the collection computed in res2VAL
        for (item <- resVAL) {
            println(item)
        }

    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val longitude: Int = 75

        // 2. Local or Databricks
        val localFalseDatabricksTrue = false

        // 3. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/1_Spark_Core/my_dataset/"

        if (localFalseDatabricksTrue == false) {
            myDatasetDir = myLocalPath + myDatasetDir
        }
        else {
            myDatasetDir = myDatabricksPath + myDatasetDir
        }

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

        Logger.getRootLogger.setLevel(Level.WARN)
        for( index <- 1 to 10){
            printf("\n")
        }

        // 5. We call to myMain
        myMain(sc, myDatasetDir, longitude)

    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
//MyProgram.main(Array())

