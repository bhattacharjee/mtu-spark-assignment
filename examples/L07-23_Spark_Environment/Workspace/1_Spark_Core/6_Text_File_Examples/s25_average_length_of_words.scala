
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
    // INLINE FUNCTION processLine
    // ------------------------------------------
    val processLine: (String) => List[String] = (line: String) => {
        // 1. We create the output variable
        var res : List[String] = List[String]()

        // 2. We set the line to be split by " "
        var newLine = line.replace("\n", "")
        newLine = newLine.trim()
        newLine = newLine.replace("\t", " ")

        // 3. We get rid of chars not being either a letter or a " "
        var totalSize = newLine.length()
        var index = totalSize - 1

        // 3.1. We traverse all characters
        while (index >= 0) {
            // 3.1.1. We get the ord of the character at position index
            val charVal : Int = (newLine.apply(index)).toInt

            // 3.1.2. If (1) char_val is not " " and (2) char_val is not an Upper Case letter and (3) char_val is not a Lower Case letter
            if ( (charVal != 32) && ((charVal < 65) || (charVal > 90)) && ((charVal < 97) || (charVal > 122)) ){
                // 3.1.2.1. We remove the index from the sentence
                newLine = newLine.substring(0, index) + newLine.substring(index+1, totalSize)
                totalSize = totalSize - 1
            }
            // 3.1.3. If the character was an upper case letter
            else if ((charVal >= 65) && (charVal <= 90)) {
                // 3.1.3.1. We add it as lower case
                newLine = newLine.substring(0, index) + ((charVal + 32).toChar).toString + newLine.substring(index+1, totalSize)
            }

            //3.1.4. We continue with the next index
            index = index - 1
        }

        // 4. We get the list of words
        val words = (newLine.split(" ")).toList

        // 4.1. We remove the empty ones
        res = words.filter(_.length() > 0)

        // 5. We return res
        res
    }

    // ------------------------------------------
    // FUNCTION myMain
    // ------------------------------------------
    def myMain(sc: SparkContext, myDatasetDir: String, myResultDir: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD: RDD[String] = sc.textFile(myDatasetDir)

        // 2. Operation T1: Transformation 'flatMap', so as to get a new RDD ('allWordsRDD') with all the words of inputRDD.
        val allWordsRDD: RDD[String] = inputRDD.flatMap(processLine)

        // 3. Operation T2: Transformation 'map', so as to get a new RDD ('pairWordsRDD') with a pair (letter, length of word) per word of the dataset.
        val pairWordsRDD: RDD[(Char, Int)] = allWordsRDD.map( (x: String) => (x.charAt(0), x.length()) )

        // 4. Operation T3: Transformation 'combineByKey', so as to get a new RDD ('letterTotalInfo') with
        //                  a pair (letter, (num_letters, num_words)) per letter of the alphabet.
        val letterTotalInfoRDD: RDD[(Char, (Int, Int))] = pairWordsRDD.combineByKey(
            (x: Int) => (x, 1),
            (x: (Int, Int), y: Int) => (x._1 + y, x._2 + 1),
            (x: (Int, Int), y: (Int, Int)) => (x._1 + y._1, x._2 + y._2)
        )

        // 5. Operation T4: Transformation 'mapValues', so as to get the average for letter in a new RDD ('solutionRDD').
        val solutionRDD: RDD[(Char, Float)] = letterTotalInfoRDD.mapValues( (value: (Int, Int)) => (value._1).toFloat / (value._2).toFloat )

        // 6. Operation A1: Store the RDD solutionRDD into the desired folder from the DBFS.
        solutionRDD.saveAsTextFile(myResultDir)
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

        // 4. We set the path to my_dataset and my_result
        val myLocalPath = "../../"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/1_Spark_Core/my_dataset/"
        var myResultDir : String = "FileStore/tables/1_Spark_Core/my_result/"

        if (localFalseDatabricksTrue == false) {
            myDatasetDir = myLocalPath + myDatasetDir
            myResultDir = myLocalPath + myResultDir
        }
        else {
            myDatasetDir = myDatabricksPath + myDatasetDir
            myResultDir = myDatabricksPath + myResultDir
        }

        // 5. We remove my_result directory
        if (localFalseDatabricksTrue == false) {
            FileUtils.deleteDirectory(new File(myResultDir))
        }
        else {
            dbutils.fs.rm(myResultDir, true)
        }

        // 6. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 6.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 6.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 6.1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        // 6.2. Databricks Mode
        else{
            sc = SparkContext.getOrCreate()
        }
        println("\n\n\n");

        // 7. We call to myMain
        myMain(sc, myDatasetDir, myResultDir)
    }

}


//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
