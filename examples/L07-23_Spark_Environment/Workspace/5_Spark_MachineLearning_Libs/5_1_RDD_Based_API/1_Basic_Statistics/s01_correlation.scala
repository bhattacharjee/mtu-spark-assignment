
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

  // ------------------------------------------
  // INLINE FUNCTION processLine
  // ------------------------------------------
  val processLine: String => Vector = (line: String) => {
    // 1. We create the output variable
    //var res : Vector = Vector()

    // 2. We remove the end of line character
    var newLine = line.substring(0, line.length)

    // 3. We split the line by tabulator characters
    var params : Array[String] = newLine.split(";")

    // 4. We turn it into a tuple of integers
    var auxList = new Array[Double](params.length)
    val size : Int = params.length

    var index = 0
    while (index < size){
      if (params(index) != ""){
        auxList(index) = params(index).toInt
      }
      else {
        auxList(index) = "0".toInt
      }
      index = index + 1
    }

    var res = Vectors.dense(auxList)

    // 5. We return res
    res
  }

  // ------------------------------------------
  // FUNCTION myMain
  // ------------------------------------------
  def myMain(sc: SparkContext, myDatasetDir: String, correlationMethod: String): Unit = {
    // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
    val inputRDD = sc.textFile(myDatasetDir)

    // 2. Operation T1: We get an RDD of tuples
    val infoRDD = inputRDD.map(processLine)

    // 3. Operation A1: We get the correlation matrix
    val correlationMatrix = Statistics.corr(infoRDD, correlationMethod)

    // 4. We display the result
    println(correlationMatrix.toString)
  }

  // ------------------------------------------
  // PROGRAM MAIN ENTRY POINT
  // ------------------------------------------
  def main(args: Array[String]) {
    // 1. We use as many input arguments as needed
    val correlationMethod : String = "pearson"
    //val correlationMethod : String = "spearman"

    val datasetFileName : String = "pearson_2_vars_dataset.csv"
    //val datasetFileName : String = "pearson_3_vars_dataset.csv"
    //val datasetFileName : String = "spearman_2_vars_dataset.csv"

    // 2. Local or Databricks
    val localFalseDatabricksTrue = true

    // 3. We set the path to my_dataset and my_result
    val myLocalPath = "../../"
    val myDatabricksPath = "/"

    var myDatasetDir : String = "FileStore/tables/5_Spark_MachineLearning_Libs/1_Basic_Statistics/" + datasetFileName

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
    myMain(sc, myDatasetDir, correlationMethod)
  }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
MyProgram.main(Array())
