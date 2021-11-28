
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
    // FUNCTION sizeWithMapPartitions
    // ------------------------------------------
    def sizeWithMapPartitions(sc: SparkContext, myDatasetDir: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD : RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation T1: We use mapPartitions to see how many partitions are there
        val inputRDDSizes : RDD[Int] = inputRDD.mapPartitions( (partitionContentIterator: Iterator[String]) => Array(partitionContentIterator.size).iterator, true )

        // 3. Operation A1: collect the items from inputRDDSizes
        val resVAL: Array[Int] = inputRDDSizes.collect()

        // 4. We print by the screen the collection computed in res2VAL
        for (item <- resVAL) {
            println(item)
        }

    }

    // ------------------------------------------
    // FUNCTION sizeWithMapPartitionsWithIndex
    // ------------------------------------------
    def sizeWithMapPartitionsWithIndex(sc: SparkContext, myDatasetDir: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD : RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation T1: We use mapPartitions to see how many partitions are there
        val inputRDDSizes : RDD[(Int, Int)] = inputRDD.mapPartitionsWithIndex( (partitionIndex: Int, partitionContentIterator: Iterator[String]) => Array((partitionIndex, partitionContentIterator.size)).iterator , true )

        // 3. Operation A1: collect the items from inputRDDSizes
        val resVAL: Array[(Int, Int)] = inputRDDSizes.collect()

        // 4. We print by the screen the collection computed in res2VAL
        for (item <- resVAL) {
            println(item)
        }

    }

    // ------------------------------------------
    // INLINE FUNCTION myWhatToDoWithThePartition
    // ------------------------------------------
    val myWhatToDoWithThePartition: (Int, Iterator[String]) => Iterator[(Int, Int)] = (partitionIndex: Int, partitionContentIterator: Iterator[String]) => {
        //1. We create the output variable with the content of currentAccumulator
        var res: Iterator[(Int, Int)] = Array((partitionIndex, partitionContentIterator.size)).iterator

        //2. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION sizeWithInlineAndMapPartitionsWithIndex
    // ------------------------------------------
    def sizeWithInlineAndMapPartitionsWithIndex(sc: SparkContext, myDatasetDir: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD : RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation T1: We use mapPartitions to see how many partitions are there
        val inputRDDSizes : RDD[(Int, Int)] = inputRDD.mapPartitionsWithIndex( myWhatToDoWithThePartition, true )

        // 3. Operation A1: collect the items from inputRDDSizes
        val resVAL: Array[(Int, Int)] = inputRDDSizes.collect()

        // 4. We print by the screen the collection computed in res2VAL
        for (item <- resVAL) {
            println(item)
        }

    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext, myDatasetDir: String): Unit = {
        println("\n\n--- [BLOCK 1] mapPartitions to get the size of each partition ---")
        sizeWithMapPartitions(sc, myDatasetDir)

        println("\n\n--- [BLOCK 2] mapPartitionsWithIndex to get the index and size of each partition ---")
        sizeWithMapPartitionsWithIndex(sc, myDatasetDir)

        println("\n\n--- [BLOCK 3] mapPartitionsWithIndex with an Inline Function to get the index and size of each partition ---")
        sizeWithInlineAndMapPartitionsWithIndex(sc, myDatasetDir)
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed

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
        myMain(sc, myDatasetDir)

    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
//MyProgram.main(Array())

