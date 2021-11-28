
//-------------------------------------
// IMPORTS
//-------------------------------------
import java.io.File

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    // ------------------------------------------
    // INLINE FUNCTION processLine
    // ------------------------------------------
    val processLine: (String) => (Int, List[Int]) = (line: String) => {
        // 1. We create the output variable
        var res : (Int, List[Int]) = null

        // 1.1. We will output the user_id
        var user_id: Int = -1

        // 1.2. We will output the list of topics the user is subscribed to
        var user_topics: List[Int] = null

        // 2. We get the info from the line
        var newLine = line.replace("\n", "")
        user_topics = ((newLine.split(" ")).map( (x:String) => x.toInt )).toList

        user_id = user_topics.head
        user_topics = user_topics.drop(1)

        // 3. We assign res
        res = (user_id, user_topics)

        // 4. We return res
        res
    }

    // ------------------------------------------
    // INLINE FUNCTION hasNovelClick
    // ------------------------------------------
    val hasNovelClick: ((Int, (List[Int], List[Int]))) => Boolean = (myPair: (Int, (List[Int], List[Int]))) => {
        // 1. We create the output variable
        var res: Boolean = false

        // 2. We collect the elements
        val userTopics: List[Int] = (myPair._2)._1
        val userClicks: List[Int] = (myPair._2)._2

        // 3. We traverse the clicks so as to see if there is any not being in user_topics
        val size: Int = userClicks.length
        var index: Int = 0
        while ((index < size) && (res == false)) {
            //3.1.If the click is new we finish
            if (userTopics.contains(userClicks.apply(index)) == false){
                res = true
            }
            // 3.2.Otherwis we continue with the next one
            else {
                index = index + 1
            }
        }

        // 4. We output res
        res
    }

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext,
               myDatasetDir: String,
               numTimeSteps: Int,
               includePartitions: Boolean,
               numPartitions: Int): Unit = {

        // 1. Operation C1: We load user_table into
        val inputRDD: RDD[String] = sc.textFile(myDatasetDir + "table.txt")

        // 2. Operation T1: We process each line of the file so as to have the format (user_id, user_topics)
        var tableRDD: RDD[(Int, List[Int])] = inputRDD.map(processLine)

        // 3. Operation P1: We specify the way tableRDD is partitioned
        if (includePartitions == true){
            tableRDD = tableRDD.partitionBy(new HashPartitioner(numPartitions))
        }

        // 4. Operation P2: We persist tableRDD
        tableRDD.persist()

        // 5. Main Loop
        var step: Int = 1

        while (step <= numTimeSteps){
            // 5.1. We print the current time step
            println("\n\n\n--- Time Step = " + step)

            // 5.2. Operation C2: We load the next time step
            val inputStepRDD: RDD[String] = sc.textFile(myDatasetDir + "time_step_" + step + ".txt")

            // 5.3. Operation T2: We process each line of the file so as to have the format (user_id, user_topics)
            val tableStepRDD: RDD[(Int, List[Int])] = inputStepRDD.map(processLine)

            // 5.4. Operation T3: We join tableRDD and table_stepRDD
            val joinRDD: RDD[(Int, (List[Int], List[Int]))] = tableRDD.join(tableStepRDD)

            // 5.5. Operation T4: We filter the ones with novel clicks
            val usersWithNovelClicksRDD: RDD[(Int, (List[Int], List[Int]))] = joinRDD.filter(hasNovelClick )

            // 5.6. Operation A1: We count how many users are there
            val resVAL: Long = usersWithNovelClicksRDD.count()

            // 5.7. We display this info
            println(resVAL)

            // 5.8. We join tableRDD and table_stepRDD
            step = step + 1
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. We use as many input arguments as needed
        val numTimeSteps: Int = 8
        val includePartitions: Boolean = true
        val numPartitions: Int = 8

        // 2. Local or Databricks
        val localFalseDatabricksTrue = true

        // 3. We setup the log level
        val logger = org.apache.log4j.Logger.getLogger("org")
        logger.setLevel(Level.WARN)

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/1_Spark_Core/click_counts_dataset/"

        if (localFalseDatabricksTrue == false) {
            myDatasetDir = myLocalPath + myDatasetDir
        }
        else {
            myDatasetDir = myDatabricksPath + myDatasetDir
        }

        // 5. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 5.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 5.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 5.1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        // 5.2. Databricks Mode
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 6. We call to myMain
        myMain(sc, myDatasetDir, numTimeSteps, includePartitions, numPartitions)
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
